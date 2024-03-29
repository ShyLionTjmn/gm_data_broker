package main

import (
  "fmt"
  "sync"
  "os"
  "log"
  "time"
  "syscall"
  "os/signal"
  "regexp"
  "errors"
  "strings"
  "strconv"
  "context"
  "encoding/json"
  "net/http"
  "flag"

  "github.com/gomodule/redigo/redis"
  "github.com/marcsauter/single"

  w "github.com/jimlawless/whereami"
  // "github.com/davecgh/go-spew/spew"
  "github.com/fatih/color"

  . "github.com/ShyLionTjmn/mapaux"
  . "github.com/ShyLionTjmn/gomapper_aux"
  "github.com/ShyLionTjmn/redsub"
  //"github.com/ShyLionTjmn/redmutex"

)

//const WARN_AGE=300
const DEAD_AGE=300

const DB_REFRESH_TIME= 10
const DB_ERROR_TIME= 5

const AUX_DATA_REFRESH=10

const ERROR_SLEEP=15
const IDLE_SLEEP=600

const REDIS_SOCKET="/tmp/redis.sock"
const REDIS_DB="0"
const REDIS_ERR_SLEEP=5

var red_db string=REDIS_DB

const IP_REGEX=`^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$`
var ip_reg *regexp.Regexp

var globalMutex = &sync.RWMutex{}
//locks this maps:
var devs = make(M)
var devs_macs = make(M)
var devs_arp = make(M)
var data = make(M)
var l2Matrix = make(M) // working map with alternatives
var dev_refs = make(M) // device references for faster lookups
var graph_int_rules string
var graph_int_rules_time int64
var graph_int_watch_dev []string
var graph_int_watch_int []string
var graph_int_watch_dev_ne []string
var graph_int_watch_int_ne []string
var alert_fields []string
var ip_neighbours_rule string
var ip_neighbours_fields []string
var ip_neighbours_ignored map[string]struct{}

var aux_data_time int64


var opt_Q bool
var opt_P bool
var opt_1 bool
var opt_v int
var opt_l bool
var opt_n bool

const TRY_OPEN_FILES uint64=65536
var max_open_files uint64

func init() {
  data["l2_links"] = make(M) // exported map with actual links. Keep link with down (2) state if both devices in db and no neighbours and any of it is down or interface is down
  data["l3_links"] = make(M)
  data["dev_list"] = make(M)
  data["sysoids"] = make(M)

  w.WhereAmI()
  errors.New("")
  strconv.Itoa(0)

  ip_reg = regexp.MustCompile(IP_REGEX)

  flag.BoolVar(&opt_Q, "Q", false, "ignore queue saves from gomapper")
  flag.BoolVar(&opt_P, "P", false, "No periodic status update for outdated devs")
  flag.BoolVar(&opt_1, "1", false, "startup and finish")
  flag.BoolVar(&opt_l, "l", false, "log link discovery and change")
  flag.BoolVar(&opt_n, "n", false, "auto add ip Neighbours")
  flag.IntVar(&opt_v, "v", 0, "set verbosity level")

  flag.Parse()
}

var red_state_mutex = &sync.Mutex{}
//locks this vars
var red_good int64
var red_bad int64

func redState(ok bool) {
  red_state_mutex.Lock()
  defer red_state_mutex.Unlock()

  if ok {
    if red_good < red_bad {
      red_good = time.Now().Unix()
      fmt.Fprintln(os.Stderr, "redis is back")
    }
  } else {
    if red_bad <= red_good {
      red_bad = time.Now().Unix()
      fmt.Fprintln(os.Stderr, "redis is down")
    }
  }
}

func read_devlist (red redis.Conn) (M, error) {
  ret := make(M)
  var err error
  var hash map[string]string

  hash, err = redis.StringMap(red.Do("HGETALL", "dev_list"))
  if err != nil { return nil, err }

  for ip, val := range hash {
    a := strings.Split(val, ":")
    if len(a) == 2 && ip_reg.MatchString(ip) && a[1] != "ignore" {
      var t int64
      t, err = strconv.ParseInt(a[0], 10, 64)
      if err == nil && t <= time.Now().Unix() {
        ret[ip] = make(M)
        ret[ip].(M)["time"] = t
        ret[ip].(M)["state"] = a[1]
      }
    }
  }

  return ret, nil
}

func queue_data_sub(stop_ch chan string, wg *sync.WaitGroup) {
  //defer func() { r := recover(); if r != nil { fmt.Println("queue_data_sub: recover from:", r) } }()
  //defer func() { fmt.Println("queue_data_sub: return") }()
  defer wg.Done()

  var err error

  stop_signalled := false

  for !stop_signalled {

    var rsub *redsub.Redsub
    rsub, err = redsub.New("unix", REDIS_SOCKET, red_db, "queue_saved", 100)
    if err == nil {
      redState(true)
L66:  for !stop_signalled {
        select {
        case <- stop_ch:
          stop_signalled = true
          //fmt.Println("queue_data_sub: quit")
          rsub.Conn.Close()
          break L66
        case err = <-rsub.E:
          if !stop_signalled {
            //fmt.Println("subscriber got error: "+err.Error())
          }
          break L66
        case reply := <-rsub.C:
          a := strings.Split(reply, ":")
          if len(a) >= 2 && a[0] == "0" && ip_reg.MatchString(a[1]) && !opt_Q {
            wg.Add(1)
            go process_ip_data(wg, a[1], false)
          }
          //fmt.Println(time.Now().Format("15:04:05"), reply)
        }
      }
      rsub.W.Wait()
      if !stop_signalled { redState(false) }
    } else {
      if !stop_signalled {
        redState(false)
        if opt_v > 0 {
          color.Red("subscriber returned error: %s", err.Error())
        }
      }
    }
  // something went wrong, sleep for a while


    if !stop_signalled {
      timer := time.NewTimer(REDIS_ERR_SLEEP*time.Second)
      select {
      case <- stop_ch:
        timer.Stop()
        if opt_v > 0 {
          fmt.Println("queue_data_sub: quit while error wait")
        }
        return
      case <- timer.C:
        //do nothing, try whole cycle again
      }
    }
  }
}

func myHttpHandlerRoot(w http.ResponseWriter, req *http.Request) {
  req.ParseForm()
  globalMutex.RLock()

  //var j []byte
  var err error

  out := make(M)

  if req.URL.Path == "/debug" || req.URL.Path == "/debug/" {
    out["data"] = data
    out["l2Matrix"] = l2Matrix
  } else if req.URL.Path == "/refs" || req.URL.Path == "/refs/" {
    out = dev_refs
  } else if req.URL.Path == "/macs" {
    out["macs"]=devs_macs
    out["arp"]=devs_arp
  } else if req.URL.Path == "/command" {
    command := req.Form.Get("command")
    if command == "" {
      out["error"] = "no command supplied"
    } else if command == "delete_l2_link" {
      link_id := req.Form.Get("link_id")
      if link_id == "" {
        out["error"] = "no link_id supplied"
      } else {
        globalMutex.RUnlock()
        globalMutex.Lock()

        if link_h, ok := data.VMe("l2_links", link_id); ok {
          matrix_id := link_h.Vs("matrix_id")
          alt_matrix_id := link_h.Vs("alt_matrix_id")
          for _, leg := range []string{"0", "1"} {
            ifName := link_h.Vs(leg, "ifName")
            DevId := link_h.Vs(leg, "DevId")

            if if_h, ok := devs.VMe(DevId, "interfaces", ifName); ok && if_h.EvA("l2_links") {
              list := if_h.VA("l2_links").([]string)
              new_list := make([]string, 0)
              for _, l := range list {
                if l != link_id {
                  new_list = append(new_list, l)
                }
              }
              if len(new_list) > 0 {
                if_h["l2_links"] = new_list
              } else {
                delete(if_h, "l2_links")
              }
            }

            if refs_h, ok := dev_refs.VMe(DevId, "l2_links"); ok {
              delete(refs_h, link_id)
              if len(refs_h) == 0 {
                delete(dev_refs.VM(DevId), "l2_links")
              }
            }
            if refs_h, ok := dev_refs.VMe(DevId, "l2Matrix"); ok {
              delete(refs_h, matrix_id)
              delete(refs_h, alt_matrix_id)
              if len(refs_h) == 0 {
                delete(dev_refs.VM(DevId), "l2Matrix")
              }
            }
          }

          delete(l2Matrix, matrix_id)
          delete(l2Matrix, alt_matrix_id)
          delete(data.VM("l2_links"), link_id)
          out["ok"] = "done"
        } else {
          out["warn"] = "no link"
        }

        globalMutex.Unlock()
        globalMutex.RLock()
      }
    } else {
      out["error"] = "unknown command supplied"
    }
  } else if req.URL.Path == "/compact" {
    _, with_macs := req.Form["with_macs"]
    _, with_arp := req.Form["with_arp"]
    ret_devs := out.MkM("devs")

    if _, ok := req.Form["with_l2_links"]; ok {
      out_l2_links := out.MkM("l2_links")
      if data.EvM("l2_links") {
        for link_id, link_m := range data.VM("l2_links") {
          out_l2_link_h := out_l2_links.MkM(link_id)
          for _, key := range []string{"status"} {
            if _a, ok := link_m.(M).VAe(key); ok {
              out_l2_link_h[key] = _a
            }
          }
          for _, leg := range []string{"0", "1"} {
            if leg_h, ok := link_m.(M).VMe(leg); ok {
              out_l2_link_leg_h := out_l2_link_h.MkM(leg)
              for _, leg_key := range []string{"DevId", "ifName"} {
                if key_val, ok := leg_h.VAe(leg_key); ok {
                  out_l2_link_leg_h[leg_key] = key_val
                }
              }
            }
          }
        }
      }
    }

    for dev_id, dev_m := range devs {
      dev_h := dev_m.(M)
      out_dev_h := ret_devs.MkM(dev_id)

      // copy scalar values and slices
      for _, key := range []string{"data_ip", "dhcpSnoopingEnable", "dhcpSnoopingStatisticDropPktsNum", "dhcpSnoopingVlanEnable", "id", "last_seen", "memorySize", "memoryUsed",
        "model_long", "model_short", "overall_status", "short_name", "sysContact", "sysDescr", "sysLocation", "sysObjectID", "sysUpTime", "sysUpTimeStr",
        "CiscoConfChange", "CiscoConfSave", "powerState",
        "interfaces_sorted"} {
        //for
        if _a, ok := dev_h.VAe(key); ok {
          out_dev_h[key] = _a
        }
      }

      // link hashes
      for _, key := range []string{"CPUs"} {
        if _h, ok := dev_h.VMe(key); ok {
          out_dev_h[key] = _h
        }
      }

      if dev_h.EvM("interfaces") {
        for ifName, if_m := range dev_h.VM("interfaces") {
          if_h := if_m.(M)

          out_if_h := out_dev_h.MkM("interfaces", ifName)

          // copy scalar values and slices
          for _, key := range []string{"ifAdminStatus", "ifAlias", "ifInCRCErrors", "ifIndex", "ifName", "ifOperStatus", "ifPhysAddr", "ifSpeed", "ifType", "ifHighSpeed",
            "macs_count", "portHybridTag", "portHybridUntag", "portIndex", "portMode", "portPvid", "portTrunkVlans", "ifDelay",
            "ip_neighbours", "l2_links", "stpBlockInstances" } {
            //for
            if _a, ok := if_h.VAe(key); ok {
              out_if_h[key] = _a
            }
          }

          // link hashes
          for _, key := range []string{"ips"} {
            //for
            if _h, ok := if_h.VMe(key); ok {
              out_if_h[key] = _h
            }
          }
        }
      }

      if dev_h.EvM("lldp_ports") {
        for portIndex, port_m := range dev_h.VM("lldp_ports") {
          port_h := port_m.(M)

          out_port_h := out_dev_h.MkM("lldp_ports", portIndex)

/*
          // copy scalar values and slices
          for _, key := range []string{} {
            //for
            if _a, ok := port_h.VAe(key); ok {
              out_port_h[key] = _a
            }
          }
*/
          // link neighbours
          if nei_h, ok := port_h.VMe("neighbours"); ok {
            out_nei_h := out_port_h.MkM("neighbours")
            for nei_index, nei_m := range nei_h {
              _ = out_nei_h.MkM(nei_index)
              _ = nei_m.(M)
            }
          }
        }
      }

      if with_macs && devs_macs.EvM(dev_id) {
        for ifName, macs_m := range devs_macs.VM(dev_id) {
          if if_h, ok := ret_devs.VMe(dev_id, "interfaces", ifName); ok {
            if_h["macs"] = macs_m
          }
        }
      }
      if with_arp && devs_arp.EvM(dev_id) {
        for ifName, arp_m := range devs_arp.VM(dev_id) {
          if if_h, ok := ret_devs.VMe(dev_id, "interfaces", ifName); ok {
            if_h["arp_table"] = arp_m
          }
        }
      }
    }
  } else if req.URL.Path == "/" {
    _, with_macs := req.Form["with_macs"]
    _, with_arp := req.Form["with_arp"]
    var short_name_regex *regexp.Regexp
    short_name_pattern := req.Form.Get("match_short_name")
    by_dev_id := req.Form.Get("dev_id")
    by_dev_ip := req.Form.Get("dev_ip")
    by_safe_dev_id := req.Form.Get("safe_dev_id")
    err = nil
    if short_name_pattern != "" {
      short_name_regex, err = regexp.Compile(short_name_pattern)
    }
    if err == nil {
      ret_devs := out.MkM("devs")

      if _, ok := req.Form["with_l2_links"]; ok {
        out["l2_links"] = data.VM("l2_links")
      }

      for dev_id, dev_m := range devs {
        dev_h := dev_m.(M)
        safe_dev_id := SafeDevId(dev_id)
        if (short_name_pattern == "" || short_name_regex.MatchString(dev_h.Vs("short_name"))) &&
           (by_dev_id == "" || by_dev_id == dev_id) &&
           (by_dev_ip == "" || by_dev_ip == dev_h.Vs("data_ip")) &&
           (by_safe_dev_id == "" || by_safe_dev_id == safe_dev_id) &&
           true {
          //if
          ret_devs[dev_id] = dev_h.Copy()

          if with_macs && devs_macs.EvM(dev_id) {
            for ifName, macs_m := range devs_macs.VM(dev_id) {
              if if_h, ok := ret_devs.VMe(dev_id, "interfaces", ifName); ok {
                if_h["macs"] = macs_m
              }
            }
          }
          if with_arp && devs_arp.EvM(dev_id) {
            for ifName, arp_m := range devs_arp.VM(dev_id) {
              if if_h, ok := ret_devs.VMe(dev_id, "interfaces", ifName); ok {
                if_h["arp_table"] = arp_m
              }
            }
          }
        }
      }
    }

  } else {
    globalMutex.RUnlock()
    http.Error(w, "Not found", http.StatusNotFound)
    return
  }

  if err != nil {
    globalMutex.RUnlock()
    http.Error(w, err.Error(), http.StatusInternalServerError)
    return
  }

  w.Header().Set("Content-Type", "text/javascript")
  w.WriteHeader(http.StatusCreated)
  enc := json.NewEncoder(w)

  if _, indent := req.Form["indent"]; indent {
    enc.SetIndent("", "  ")
  }
  enc.Encode(out)
  //w.Write([]byte("\n"))
  globalMutex.RUnlock()
}


func http_server(stop chan string, wg *sync.WaitGroup) {
  defer wg.Done()
  s := &http.Server{
    Addr:       ":8181",
  }

  server_shut := make(chan struct{})

  go func() {
    <-stop
    if opt_v > 0 {
      fmt.Println("Shutting down HTTP server")
    }
    ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(500 * time.Millisecond))
    defer cancel()

    shut_err := s.Shutdown(ctx)
    if shut_err != nil {
      if opt_v > 0 {
        color.Red("HTTP server Shutdown error: %v\n", shut_err)
      }
    }
    close(server_shut)
  }()

  http.HandleFunc("/", myHttpHandlerRoot)

  http_err := s.ListenAndServe()
  if http_err != http.ErrServerClosed {
    if opt_v > 0 {
      color.Red("HTTP server shot down with error: %s", http_err)
    }
  }
  <-server_shut
}

func main() {

  var err error
  var sysoids_time string
  var alert_config_time string
  var ip_neighbours_time string

  single_run := single.New("gm_data_broker."+red_db) // add redis_db here later

  if err = single_run.CheckLock(); err != nil && err == single.ErrAlreadyRunning {
    log.Fatal("another instance of the app is already running, exiting")
  } else if err != nil {
    // Another error occurred, might be worth handling it as well
    log.Fatalf("failed to acquire exclusive app lock: %v", err)
  }
  defer single_run.TryUnlock()

  var rLimit syscall.Rlimit
  err = syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
  if err != nil {
    fmt.Fprintf(os.Stderr, "Error getting ulimit")
    return
  }

  max_open_files = rLimit.Cur

  if rLimit.Max != rLimit.Cur {
    rLimit.Cur = rLimit.Max
  }

  err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
  if err != nil {
    fmt.Fprintf(os.Stderr, "Error raising ulimit")
  } else {
    max_open_files = rLimit.Cur

    if rLimit.Cur < TRY_OPEN_FILES {
      rLimit.Cur = TRY_OPEN_FILES
      rLimit.Max = TRY_OPEN_FILES

      err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
      if err == nil {
        max_open_files = rLimit.Cur
      }
    }
  }


  //fmt.Println("Max open files:", max_open_files)

  sig_ch := make(chan os.Signal, 1)
  signal.Notify(sig_ch, syscall.SIGHUP)
  signal.Notify(sig_ch, syscall.SIGINT)
  signal.Notify(sig_ch, syscall.SIGTERM)
  signal.Notify(sig_ch, syscall.SIGQUIT)


  var wg sync.WaitGroup
  var stop_channels []chan string

  redis_loaded := false
  queue_data_sub_launched := false
  http_launched := false

  var red redis.Conn

  defer func() { if red != nil { red.Close() } } ()

  var report_once sync.Once

MAIN_LOOP:
  for {

    red, err = RedisCheck(red, "unix", REDIS_SOCKET, red_db)

    redState(red != nil && err == nil)

    if red != nil {
      var redstr string
      redstr, err = redis.String(red.Do("GET", "graph_int_rules"))
      if err == nil {
        globalMutex.Lock()
        if redstr != graph_int_rules {
          if d, dne, i, ine, _err := ParseGraphIntRules(redstr); err == nil {
            graph_int_rules = redstr
            graph_int_rules_time = time.Now().Unix()
            graph_int_watch_dev = d
            graph_int_watch_int = i
            graph_int_watch_dev_ne = dne
            graph_int_watch_int_ne = ine
          } else {
            if opt_v > 1 {
              color.Red("Error parsing graph_int_rules: %s", _err.Error())
            }
          }
        }
        globalMutex.Unlock()
      }
    }

    if (aux_data_time + AUX_DATA_REFRESH) < time.Now().Unix() && red != nil && red.Err() == nil {
      aux_data_time = time.Now().Unix()
      var r_time string
      r_time, err = redis.String(red.Do("HGET", "sysoids.short", "time"))
      if err == nil && r_time != sysoids_time {
        var redmap map[string]string
        redmap, err = redis.StringMap(red.Do("HGETALL", "sysoids.short"))
        redmap2, err2 := redis.StringMap(red.Do("HGETALL", "sysoids.long"))
        if err == nil && err2 == nil {
          globalMutex.Lock()
          sysoids_time = r_time
          delete(data, "sysoids")
          data["sysoids"] = make(M)
          for key, short := range redmap {
            if long, ok := redmap2[key]; ok {
              data.VM("sysoids")[key] = make(M)
              data.VM("sysoids", key)["short"] = short
              data.VM("sysoids", key)["long"] = long
            }
          }
          globalMutex.Unlock()
        }
      }
      r_time, err = redis.String(red.Do("HGET", "alert_config", "time"))
      if err == nil && r_time != alert_config_time {
        var redmap map[string]string
        redmap, err = redis.StringMap(red.Do("HGETALL", "alert_config"))
        if err == nil {
          new_fields := make([]string, 0)
          for key, rule := range redmap {
            if dotpos := strings.Index(key, "."); dotpos > 0 && len(key[dotpos+1:]) > 0 {
              if key[:dotpos] == "rule" || key[:dotpos] == "group" {
                var rule_fields []string
                rule_fields, err = ParseAlertRule(rule)
                if err != nil {
                  break
                }
                for _, field := range rule_fields {
                  new_fields = StrAppendOnce(new_fields, field)
                }
              }
            }
          }
          if err == nil {
            alert_config_time = r_time
            globalMutex.Lock()
            alert_fields = new_fields
            globalMutex.Unlock()
          }
        }
      }
      r_time, err = redis.String(red.Do("GET", "config.ip_neighbours.time"))
      if err == nil && r_time != ip_neighbours_time {
        var redstr string
        redstr, err = redis.String(red.Do("GET", "config.ip_neighbours.rule"))
        if err == nil {
          var new_fields []string
          new_fields, err = ParseAlertRule(redstr)
          if err == nil {
            globalMutex.Lock()
            ip_neighbours_time = r_time
            ip_neighbours_rule = redstr
            ip_neighbours_fields = new_fields
            ip_neighbours_ignored = make(map[string]struct{})
            globalMutex.Unlock()
          }
        }
      }
    }

    if !redis_loaded && red != nil && red.Err() == nil {
      var dev_map M

      dev_map, err = read_devlist(red)
      if err == nil {
        total_ips := uint64(len(dev_map))
        fast_start := max_open_files > total_ips+20
        var wg_ sync.WaitGroup
        for ip, _ := range dev_map {
          if ip_reg.MatchString(ip) && dev_map.Vs(ip, "state") != "conflict" {
            if opt_v > 1 {
              fmt.Println("Load IP", ip)
            }
            if fast_start {
              wg_.Add(1)
              go process_ip_data(&wg_, ip, true)
            } else {
              process_ip_data(nil, ip, true)
            }
          }
        }
        if fast_start { wg_.Wait() }
        redis_loaded = true

        globalMutex.Lock()
        for _, dev_m := range devs {
          ip_debug, _ := redis.String(red.Do("GET", "ip_debug."+dev_m.(M).Vs("data_ip")))
          processLinks(red, dev_m.(M), true, ip_debug)
        }
        globalMutex.Unlock()
      }
    }

//if queue_data_sub_launched {}
    if redis_loaded && !queue_data_sub_launched {
      if opt_v > 0 {
        fmt.Println("Start processing live reports")
      }
      queue_data_sub_stop := make(chan string, 1)
      stop_channels = append(stop_channels, queue_data_sub_stop)

      wg.Add(1)
      queue_data_sub_launched = true
      go queue_data_sub(queue_data_sub_stop, &wg)
    }

    if opt_1 {
      //leave so soon?
      break MAIN_LOOP
    }

    if redis_loaded && !http_launched {
      if opt_v > 0 {
        fmt.Println("Starting http listener")
      }
      _stop_ch := make(chan string, 1)
      stop_channels = append(stop_channels, _stop_ch)

      wg.Add(1)
      http_launched = true
      go http_server(_stop_ch, &wg)
    }

    if redis_loaded && queue_data_sub_launched && http_launched {
      report_once.Do(func() { fmt.Println("ready to serve") })
    }
    main_timer := time.NewTimer(DB_REFRESH_TIME * time.Second)

    select {
    case s := <-sig_ch:
      main_timer.Stop()
      if opt_v > 0 {
        fmt.Println("\nmain got signal")
      }
      if s != syscall.SIGHUP && s != syscall.SIGUSR1 {
        break MAIN_LOOP
      }
      continue MAIN_LOOP
    case <- main_timer.C:
      if redis_loaded && red != nil && red.Err() == nil && !opt_P{
        if opt_v > 2 {
          fmt.Println("main timer: cleanup and status check")
        }
        var dev_map M
        dev_map, err = read_devlist(red)
        if err != nil {
          continue MAIN_LOOP
        }

        var gomapper_run int64 = 0
        var redstr string
        redstr, err = redis.String(red.Do("GET", "gomapper.run"))
        if err == nil {
          var gm_start int64
          var gm_last int64
          a := strings.Split(redstr, ":")
          if len(a) == 2 {
            gm_start, err = strconv.ParseInt(a[0], 10, 64)
            if err != nil { continue MAIN_LOOP }
            gm_last, err = strconv.ParseInt(a[1], 10, 64)
            if err != nil { continue MAIN_LOOP }
            gomapper_run = gm_last - gm_start
          }
        }
        if err == redis.ErrNil { err = nil }
        if err != nil { continue MAIN_LOOP }

        globalMutex.Lock()

        //check for deleted ips from dev_list
        for ip, _ := range data.VM("dev_list") {
          if _, ok := dev_map[ip]; !ok {
            //no such ip in redis dev_list
            if dev_id, ok := data.Vse("dev_list", ip, "id"); ok {

              if opt_v > 1 {
                fmt.Println("main timer: wipe dev:", dev_id, "ip:", ip)
              }
              wipe_dev(dev_id)
            }
            delete(data.VM("dev_list"), ip)
          }
        }

        now_unix := time.Now().Unix()

//L466:   for dev_id, _ := range devs {
        for dev_id, _ := range devs {
          ip := devs.Vs(dev_id, "data_ip")
          //check if dev ip is not in lists
          if _, ok := dev_map[ip]; !ok || !data.EvM("dev_list", ip) {
            if opt_v > 1 {
              fmt.Println("main timer: wipe dev:", dev_id, "ip:", ip)
            }
            wipe_dev(dev_id)
            delete(data.VM("dev_list"), ip)
          } else {
            /*if opt_v > 2 {
              fmt.Println("main timer: status check:", dev_id, "ip:", ip)
              fmt.Println("\tgomapper_run:", gomapper_run)
              fmt.Println("\tdev time age:", now_unix - devs.Vi(dev_id, "last_seen"))
              fmt.Println("\tdev_list time age:", now_unix - dev_map.Vi(ip, "time"))
              fmt.Println("\tdev_list state:", dev_map.Vs(ip, "state"))
            }*/
            // Process ip data to generte WARN/ERROR status
            //if gomapper_run > 90 && (now_unix - devs.Vi(dev_id, "last_seen")) > WARN_AGE &&
            if gomapper_run > 90 && (now_unix - devs.Vi(dev_id, "last_seen")) > DEAD_AGE &&
                    (now_unix - dev_map.Vi(ip, "time")) > 90 && dev_map.Vs(ip, "state") == "run" {
              wg.Add(1)
              go process_ip_data(&wg, ip, false)
            }
          }
        }

        globalMutex.Unlock()
      }
      //restart main loop
      continue MAIN_LOOP
    }
  } //MAIN_LOOP

  for _, ch := range stop_channels {
    //ch <- "stop"
    close(ch)
  }
  if WaitTimeout(&wg, 5*time.Second) {
    fmt.Println("main wait timed out")
  }

  fmt.Println("main done")
}
