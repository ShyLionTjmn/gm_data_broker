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

  "github.com/gomodule/redigo/redis"
  "github.com/marcsauter/single"

  // "github.com/davecgh/go-spew/spew"

  . "github.com/ShyLionTjmn/aux"
  . "github.com/ShyLionTjmn/decode_dev"
  "github.com/ShyLionTjmn/redsub"

)

const WARN_AGE=300
const DB_REFRESH_TIME= 10
const DB_ERROR_TIME= 5

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
var data = make(M)
var data["l2_links"] = make(M) // exported map with actual links. Keep link with down (2) state if both devices in db and no neighbours and any of it is down or interface is down
var data["l3_links"] = make(M)
var data["dev_list"] = make(M)
var l2Matrix = make(M) // working map with alternatives


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

func process_ip_data(wg *sync.WaitGroup, ip string, startup bool) {
  if wg != nil {
    defer wg.Done()
  }
  var err error
  var raw M

  var red redis.Conn

  red, err = RedisCheck(red, "unix", REDIS_SOCKET, red_db)

  if red == nil { return }

  defer func() { if red != nil { red.Close() } }()

  defer func() {
    if err != nil && red != nil && red.Err() == nil {
      ip_err := fmt.Sprintf("%d:%s ! %s", time.Now().Unix(), time.Now().Format("2006 Jan 2 15:04:05"), err.Error())
      red.Do("SET", "ip_proc_error."+ip, ip_err)
    }
  }()

  var dev_list_state string
  dev_list_state, err = redis.String(red.Do("HGET", "dev_list", ip))
  if err != nil {
    if err == redis.ErrNil {
      //device removed from dev_list, just ignore it
      err = nil

      if !startup {
        //we should clean data
      }
    }
    return
  }

  raw, err = GetRawRed(red, ip)
  if err != nil { return }

  device := Dev{ Opt_m: false, Opt_a: false, Dev_ip: ip }

  err = device.Decode(raw)
  if err != nil { return }

  dev := device.Dev

  if !dev.Evs("id") {
    err = errors.New("No id key")
    return
  }

  queue_list := dev.VA("_queues")
  if queue_list == nil || len(queue_list.([]string)) == 0 {
    err = errors.New("No _queues key")
    return
  }

  last_seen := int64(0)
  overall_status := "ok"
  if dev_list_state != "run" {
    overall_status = "paused"
  }

  for _, q := range queue_list.([]string) {
    if !dev.Evs("_last_result", q) {
      err = errors.New("No _last_result for queue "+q)
      return
    }

    lr := dev.Vs("_last_result", q)
    if strings.Index(lr, "ok:") != 0 {
      if overall_status == "ok" {
        overall_status = "error"
      }
    } else {
      a := strings.Split(lr, ":")
      if len(a) != 3 || !IsNumber(a[2]) {
        err = errors.New("Bad last_result format for queue "+q)
        return
      }
      queue_save_time, _ := strconv.ParseInt(a[2], 10, 64)
      if queue_save_time > last_seen {
        last_seen = queue_save_time
      }
    }
  }

  dev["last_seen"] = last_seen

  if (time.Now().Unix() - last_seen) > WARN_AGE && overall_status == "ok" {
    overall_status = "warn"
  }

  dev["overall_status"] = overall_status

  dev_id := dev.Vs("id")

  globalMutex.Lock()
  defer globalMutex.Unlock()

  if devs.EvM(dev_id) && devs.Vs(dev_id, "dev_ip") != ip {
    conflict_ip := devs.Vs(dev_id, "dev_ip")
    //there is duplicate device id
    if overall_status == "ok" && devs.Vs(dev_id, "overall_status") != "ok" {
      // duplicate device is old, overwrite it
      delete(devs, dev_id)
      ip_err := fmt.Sprintf("%d:%s ! %s", time.Now().Unix(), time.Now().Format("2006 Jan 2 15:04:05"), "Pausing due to conflict with running device "+ip)
      red.Do("SET", "ip_proc_error."+conflict_ip, ip_err)
      red.Do("HSET", "dev_list", conflict_ip, "conflict")
    } else if devs.Vs(dev_id, "overall_status") == "ok" && overall_status != "ok" {
      // this device is old or paused, ignore data
      red.Do("HSET", "dev_list", ip, "conflict")
      err = errors.New("Conflict with running dev "+conflict_ip+". Pausing. Prev status was: "+overall_status)
      return
    } else {
      //both good or both bad. compare last_seen
      if last_seen > devs.Vi(dev_id, "last_seen") {
        //this dev is more recent
        delete(devs, dev_id)
        ip_err := fmt.Sprintf("%d:%s ! %s", time.Now().Unix(), time.Now().Format("2006 Jan 2 15:04:05"), "Pausing due to conflict with more recent device "+ip)
        red.Do("SET", "ip_proc_error."+conflict_ip, ip_err)
        red.Do("HSET", "dev_list", conflict_ip, "conflict")
      } else {
        //this dev data is older
        red.Do("HSET", "dev_list", ip, "conflict")
        err = errors.New("Conflict with more recent dev "+conflict_ip+". Pausing. Prev status was: "+overall_status)
        return
      }
    }
  }

  // process links
  check_matrix := make(M)

  if dev.EvM("lldp_ports") && dev.Evs("locChassisId") {
    for port_index, port_h := range dev.VM("lldp_ports") {
      if port_h.(M).EvM("neighbours") {
        for seq, nei_h := range port_h.(M).VM("neighbours") {
          rcid := nei_h.(M).Vs("RemChassisId")
          rport_id := nei_h.(M).Vs("RemPortId")
          norm_matrix_id := l2l_key(dev.Vs("locChassisId"), port_h.(M).Vs("port_id"), rcid, rport_id)
          alt_matrix_id := norm_matrix_id //SNR bug, when port id is ifName in PDU, but ifIndex in locPortID
          if port_h.(M).Evs("ifName") {
            alt_matrix_id = l2l_key(dev.Vs("locChassisId"), port_h.(M).Vs("ifName"), rcid, rport_id)
          }

          matrix_id := norm_matrix_id

          if !l2Matrix.EvM(matrix_id) && l2Matrix.EvM(alt_matrix_id) {
            matrix_id = alt_matrix_id
          }

          if l2Matrix.EvM(matrix_id) {
            matrix_h := l2Matrix.VM(matrix_id)
            if matrix_h.Vs("_creator") == dev_id {
              if matrix_h.Vi("_complete") == 1 {
                matrix_h["_time"] = last_seen
                check_matrix[matrix_id] = dev_id
              }
              continue
            }
            if matrix_h.Vi("_complete") == 1 {
              continue
            }
            leg1_h := matrix_h.VM("1")
            leg1_h["PortIndex"] = port_index
            leg1_h["DevId"] = dev_id

            matrix_h["link_id"]=l2l_key(matrix_h.VM("0")["DevId"], matrix_h.VM("0")["PortIndex"], matrix_h.VM("1")["DevId"], matrix_h.VM("1")["PortIndex"])

            if port_h.(M).Evs("ifName") {
              leg1_h["ifName"] = port_h.(M).Vs("ifName")
              if matrix_h.Evs("0", "ifName") {
                rifname := matrix_h.Vs("0", "ifName")
                rdevid := matrix_h.Vs("0", "DevId")

                if devs.EvM(rdevid, "interfaces", rifname) &&
                   matrix_h.Vi("_time") == devs.Vi(rdevid, "last_seen") {
                  // if
                  matrix_h["_complete"] = int64(1)
                  check_matrix[matrix_id] = rdevid
                }
              }
            }


          } else {
            norm_matrix_h := l2Matrix.MkM(norm_matrix_id)

            norm_matrix_h["_creator"] = dev_id
            norm_matrix_h["_complete"] = int64(0)
            norm_matrix_h["_alt"] = int64(0)
            norm_matrix_h["matrix_id"] = norm_matrix_id
            norm_matrix_h["_alt_matrix_id"] = alt_matrix_id
            norm_matrix_h["_time"] = last_seen

            leg0_h := norm_matrix_h.MkM("0")
            leg0_h["ChassisId"] = dev.Vs("locChassisId")
            leg0_h["ChassisIdSubtype"] = dev.VA("locChassisIdSubtype")
            leg0_h["PortId"] = port_h.(M).Vs("port_id")
            leg0_h["PortIdSubtype"] = port_h.(M).VA("subtype")
            leg0_h["PortIndex"] = port_index
            leg0_h["DevId"] = dev_id
            if dev.Evs("locChassisSysName") {
              leg0_h["ChassisSysName"] = dev.Vs("locChassisSysName")
            }

            if port_h.(M).Evs("ifName") {
              leg0_h["ifName"] = port_h.(M).Vs("ifName")
            }

            leg1_h := norm_matrix_h.MkM("1")
            leg1_h["ChassisId"] = rcid
            leg1_h["ChassisIdSubtype"] = nei_h.(M).VA("RemChassisIdSubtype")
            leg1_h["PortId"] = rport_id
            leg1_h["PortIdSubtype"] = nei_h.(M).VA("RemPortIdSubtype")
            leg1_h["ChassisSysName"] = nei_h.(M).VA("RemSysName")

            nei_dev_id, nei_port_index, nei_ifname, nei_error := leg_nei(norm_matrix_h["1"])
            if nei_error != legNeiErrNoDev {
              leg1_h["DevId"] = nei_dev_id
              leg1_h["PortIndex"] = nei_port_index
              norm_matrix_h["link_id"]=l2l_key(matrix_h.VM("0")["DevId"], matrix_h.VM("0")["PortIndex"], matrix_h.VM("1")["DevId"], matrix_h.VM("1")["PortIndex"])
              if nei_error != legNeiErrNoIfName {
                leg1_h["ifName"] = nei_ifname
                if port_h.(M).Evs("ifName") {
                  norm_matrix_h["_complete"] = int64(1)
                  check_matrix[norm_matrix_id] = dev_id
                }
              }
            }

            if norm_matrix_id != alt_matrix_id && norm_matrix_h["_complete"] != 1 {
              alt_matrix_h := norm_matrix_h.Copy()
              alt_matrix_h["_alt"] = int64(1)
              alt_matrix_h["_alt_matrix_id"] = norm_matrix_id
              alt_matrix_h["matrix_id"] = alt_matrix_id
              l2Matrix[alt_matrix_id] = alt_matrix_h
            }
          }
        }
      }
    }
  }

  //copy links from previous run and cleanup outdated
  if !startup && devs.EvM(dev_id, "interfaces") {
    for ifName, if_h := range devs.VM(dev_id, "interfaces") {
      if if_h.(M).EvA("l2_links") && dev.EvM("interfaces", ifName) {
        link_list := make([]string, 0)
        for _, link_id := range if_h.(M).VA("l2_links").([]string) {
          keep_link := false
          link_h := data.VM("l2_links", link_id)
          if link_h != nil {

            matrix_id := link_h.Vs("matrix_id")
            if l2Matrix.EvM(matrix_id) {
              matrix_h := l2Matrix.VM(matrix_id)
              creator := matrix_h.Vs("_creator")
              l0_ifName := matrix_h.Vs("0", "ifName")
              l1_ifName := matrix_h.Vs("1", "ifName")
              l1_devId := matrix_h.Vs("1", "DevId")
              if (creator == dev_id && devs.EvA(l1_devId, "interfaces", l1_ifName, "l2_links") ||
                 (creator != dev_id && devs.EvA(creator, "interfaces", l0_ifName, "l2_links") {
                if (creator == dev_id && matrix_h.Vi("_time") == last_seen && check_matrix.Evs(matrix_id)) ||
                   (creator != dev_id && matrix_h.Vi("_time") == devs.Vi(creator, "last_seen") ||
                   false {
                  //if
                  keep_link = true
                  matrix_h["status"] = int64(1)
                  link_h["status"] = int64(1)
                } else if (creator == dev_id &&
                           len(if_h.(M).VA("l2_links").([]string)) == 1 &&
                           len(devs.VA(l1_devId, "interfaces", l1_ifName, "l2_links").([]string)) == 1 &&
                           (dev.Vs("overall_status") != "ok" ||
                            dev.Vi("interfaces", l0_ifName, "ifOperStatus") == 2 ||
                            devs.Vs(l1_devId, "overall_status") != "ok" ||
                            devs.Vs(l1_devId, "interfaces", l1_ifName, "ifOperStatus") == 2 ||
                            false
                           )
                          ) ||
                          (creator != dev_id &&
                           len(if_h.(M).VA("l2_links").([]string)) == 1 &&
                           len(devs.VA(creator, "interfaces", l0_ifName, "l2_links").([]string)) == 1 &&
                           (dev.Vs("overall_status") != "ok" ||
                            dev.Vi("interfaces", l0_ifName, "ifOperStatus") == 2 ||
                            devs.Vs(creator, "overall_status") != "ok" ||
                            devs.Vs(creator, "interfaces", l0_ifName, "ifOperStatus") == 2 ||
                            false
                           )
                          ) ||
                          false {
                  //else if
                  keep_link = true
                  link_h["status"] = int64(2)
                  matrix_h["status"] = int64(2)
                }

              }
            }
          }
          if keep_link {
            link_list = append(link_list, link_id)
          } else {
            if link_h != nil {
              creator := link_h.Vs("_creator")
              l0_ifName := link_h.Vs("0", "ifName")
              l1_ifName := link_h.Vs("1", "ifName")
              l1_devId := link_h.Vs("1", "DevId")

              if creator == dev_id && devs.EvA(l1_devId, "interfaces", l1_ifName, "l2_links") {
                list := devs.VA(l1_devId, "interfaces", l1_ifName, "l2_links").([]string)
                found := -1
                for i := 0; i < len(old_list); i++ { if list[i] == link_id { found = i; break } }
                if found >= 0 {
                  devs.VM(l1_devId, "interfaces", l1_ifName)["l2_links"] = append(list[:found], list[found+1:]...)
                }
              }

              if creator != dev_id && devs.EvA(creator, "interfaces", l0_ifName, "l2_links") {
                list := devs.VA(creator, "interfaces", l0_ifName, "l2_links").([]string)
                found := -1
                for i := 0; i < len(old_list); i++ { if list[i] == link_id { found = i; break } }
                if found >= 0 {
                  devs.VM(creator, "interfaces", l0_ifName)["l2_links"] = append(list[:found], list[found+1:]...)
                }
              }

              matrix_id := link_h.Vs("matrix_id")

              delete(data.VM("l2_links"), link_id)

              delete(l2Matrix, matrix_id)

            }
          }
        }

        if_h.(M)["l2_links"] = link_list
      }
    }
  }


  for matrix_id, _ := range check_matrix {
    var if0_h M
    var if1_h M
    matrix_h := l2Matrix.VM(matrix_id)

    if check_matrix.Vs(matrix_id) == dev_id {
      if0_h = dev.VM("interfaces", matrix_h.Vs("0", "ifName"))
      if1_h = devs.VM(matrix_h.Vs("1", "DevId"), "interfaces", matrix_h.Vs("1", "ifName"))
    } else {
      if0_h = devs.VM(matrix_h.Vs("0", "DevId"), "interfaces", matrix_h.Vs("0", "ifName"))
      if1_h = dev.VM("interfaces", matrix_h.Vs("1", "ifName"))
    }

    if !if0_h.EvA("l2_links") {
      if0_h["l2_links"] = make([]string, 1)
    }

    found := false

    for _, _id := range if0_h["l2_links"].([]string) {
      if id == matrix_id {
        found = true
        break
      }
    }

    if !found {
      if0_h["l2_links"] = append(if0_h["l2_links"].([]string), matrix_id)
    }

    if !if1_h.EvA("l2_links") {
      if1_h["l2_links"] = make([]string, 1)
    }

    found = false

    for _, _id := range if1_h["l2_links"].([]string) {
      if id == matrix_id {
        found = true
        break
      }
    }

    if !found {
      if1_h["l2_links"] = append(if1_h["l2_links"].([]string), matrix_id)
    }
  }

  if !devs.EvM(dev_id) {
    devs[dev_id] = dev
  } else {
    // check what's changed
    devs[dev_id] = dev
  }
}

func queue_data_sub(stop_ch chan string, wg *sync.WaitGroup) {
  defer func() { r := recover(); if r != nil { fmt.Println("queue_data_sub: recover from:", r) } }()
  defer func() { fmt.Println("queue_data_sub: return") }()
  defer wg.Done()

  var err error

  stop_signalled := false

  for !stop_signalled {

    var rsub *redsub.Redsub
    rsub, err = redsub.New("unix", REDIS_SOCKET, red_db, "queue_saved")
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
          if len(a) == 2 && a[0] == "0" && ip_reg.MatchString(a[1]) {
            wg.Add(1)
            go process_ip_data(wg, a[1], false)
          }
          fmt.Println(time.Now().Format("15:04:05"), reply)
        }
      }
      rsub.W.Wait()
      if !stop_signalled { redState(false) }
    } else {
      if !stop_signalled {
        redState(false)
        fmt.Println("subscriber returned error: "+err.Error())
      }
    }
  // something went wrong, sleep for a while


    if !stop_signalled {
      timer := time.NewTimer(REDIS_ERR_SLEEP*time.Second)
      select {
      case <- stop_ch:
        timer.Stop()
        fmt.Println("queue_data_sub: quit while error wait")
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
  j, err := json.MarshalIndent(devs, "", "  ")
  globalMutex.RUnlock()

  if err != nil {
    http.Error(w, err.Error(), http.StatusInternalServerError)
    return
  }

  w.Header().Add("Content-Type", "text/javascript")
  w.Write(j)
}


func http_server(stop chan string, wg *sync.WaitGroup) {
  defer wg.Done()
  s := &http.Server{
    Addr:       ":8181",
  }

  server_shut := make(chan struct{})

  go func() {
    <-stop
    fmt.Println("Shutting down HTTP server")
    ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(500 * time.Millisecond))
    defer cancel()

    shut_err := s.Shutdown(ctx)
    if shut_err != nil {
      fmt.Printf("HTTP server Shutdown: %v\n", shut_err)
    }
    close(server_shut)
  }()

  http.HandleFunc("/", myHttpHandlerRoot)

  http_err := s.ListenAndServe()
  if http_err != http.ErrServerClosed {
    fmt.Println("HTTP server shot down with error:", http_err)
  }
  <-server_shut
}

const TRY_OPEN_FILES uint64=65536
var max_open_files uint64

func main() {

  defer func() { fmt.Println("main return") } ()

  var err error

  ip_reg = regexp.MustCompile(IP_REGEX)


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

  fmt.Println("Max open files:", max_open_files)

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

MAIN_LOOP:
  for {

    red, err = RedisCheck(red, "unix", REDIS_SOCKET, red_db)

    redState(red != nil && err == nil)

    if !redis_loaded && red != nil {
      var dev_map map[string]string

      dev_map, err = redis.StringMap(red.Do("HGETALL", "dev_list"))
      if err == nil {
        total_ips := uint64(len(dev_map))
        var wg_ sync.WaitGroup
        for ip, _ := range dev_map {
          if ip_reg.MatchString(ip) {
            //fmt.Println("Load IP", ip)
            if max_open_files > total_ips+20 {
              wg_.Add(1)
              go process_ip_data(&wg_, ip, true)
            } else {
              process_ip_data(nil, ip, true)
            }
          }
        }
        wg_.Wait()
        redis_loaded = true

      }
    }

    if redis_loaded && !queue_data_sub_launched {
      fmt.Println("Start processing live reports")
      queue_data_sub_stop := make(chan string, 1)
      stop_channels = append(stop_channels, queue_data_sub_stop)

      wg.Add(1)
      queue_data_sub_launched = true
      go queue_data_sub(queue_data_sub_stop, &wg)
    }

    if redis_loaded && !http_launched {
      fmt.Println("Starting http listener")
      _stop_ch := make(chan string, 1)
      stop_channels = append(stop_channels, _stop_ch)

      wg.Add(1)
      http_launched = true
      go http_server(_stop_ch, &wg)
    }

    main_timer := time.NewTimer(DB_REFRESH_TIME * time.Second)

    select {
    case s := <-sig_ch:
      main_timer.Stop()
      fmt.Println("\nmain got signal")
      if s != syscall.SIGHUP && s != syscall.SIGUSR1 {
        break MAIN_LOOP
      }
      continue MAIN_LOOP
    case <- main_timer.C:
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
