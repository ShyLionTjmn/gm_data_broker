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

  . "github.com/ShyLionTjmn/aux"
  . "github.com/ShyLionTjmn/gomapper_aux"
  "github.com/ShyLionTjmn/redsub"
  "github.com/ShyLionTjmn/redmutex"

)

const WARN_AGE=300
const DEAD_AGE=600

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
var devs_macs = make(M)
var devs_arp = make(M)
var data = make(M)
var l2Matrix = make(M) // working map with alternatives
var dev_refs = make(M) // device references for faster lookups
var graph_int_rules string
var graph_int_watch_dev []string={}
var graph_int_watch_int []string={}

var opt_Q bool
var opt_1 bool
var opt_v int
var opt_l bool

const TRY_OPEN_FILES uint64=65536
var max_open_files uint64

func init() {
  data["l2_links"] = make(M) // exported map with actual links. Keep link with down (2) state if both devices in db and no neighbours and any of it is down or interface is down
  data["l3_links"] = make(M)
  data["dev_list"] = make(M)

  w.WhereAmI()
  errors.New("")
  strconv.Itoa(0)

  ip_reg = regexp.MustCompile(IP_REGEX)

  flag.BoolVar(&opt_Q, "Q", false, "ignore queue saves from gomapper")
  flag.BoolVar(&opt_1, "1", false, "startup and finish")
  flag.BoolVar(&opt_l, "l", false, "log link discovery and change")
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

var graphDevKey_regex *regexp.Regexp
var graphIntKey_regex *regexp.Regexp
var graphDevNeKey_regex *regexp.Regexp
var graphIntNeKey_regex *regexp.Regexp

func init() {
  graphDevKey_regex = regexp.MustCompile(`^dev\.([0-9a-zA-Z_]+)[ \t]*(==|=~|!=|!~)[ \t]*([^\s])`)
  graphIntKey_regex = regexp.MustCompile(`^int\.([0-9a-zA-Z_]+)[ \t]*(==|=~|!=|!~)[ \t]*([^\s])`)
  graphDevNeKey_regex = regexp.MustCompile(`^not_empty +dev\.([0-9a-zA-Z_]+)\s*($|[^\s])`)
  graphIntNeKey_regex = regexp.MustCompile(`^not_empty +int\.([0-9a-zA-Z_]+)\s*($|[^\s])`)
}


func parseGraphIntRules(s string) ([]string, []string, error) {
  s_pos := 0
  ret_d := make([]string, 0)
  ret_i := make([]string, 0)

  par_open := 0

  for s_pos < len(s) {
    for s_pos < len(s) && s[s_pos] == ' ' || s[s_pos] == '\n' || s[s_pos] == '\t' { s_pos++ }
    if s_pos == len(s) {
      if par_open == 0 {
        return ret_d, ret_i, nil
      } else {
        return nil, nil, errors.New("No closing parenthesis")
      }
    }
    op := ""
    if m := graphDevKey_regex.FindStringSubmatchIndex(s[s_pos:]); m != nil {
      ret_d = append(ret_d, s[s_pos+m[2]:s_pos+m[3]])
      op = s[s_pos+m[4]:s_pos+m[5]]
      s_pos += m[6] //at least 1 symbol left in string
    } else if m := graphIntKey_regex.FindStringSubmatchIndex(s[s_pos:]); m != nil {
      ret_i = append(ret_d, s[s_pos+m[2]:s_pos+m[3]])
      op = s[s_pos+m[4]:s_pos+m[5]]
      s_pos += m[6] //at least 1 symbol left in string
    } else if m := graphDevNeKey_regex.FindStringSubmatchIndex(s[s_pos:]); m != nil {
      ret_d = append(ret_d, s[s_pos+m[2]:s_pos+m[3]])
      op = "not_empty"
      s_pos += m[4] //at first non space char or at the end of script
    } else if m := graphIntNeKey_regex.FindStringSubmatchIndex(s[s_pos:]); m != nil {
      ret_i = append(ret_d, s[s_pos+m[2]:s_pos+m[3]])
      op = "not_empty"
      s_pos += m[4] //at first non space char or at the end of script
    } else if s[s_pos] == '(' {
      par_open++
      s_pos++
    } else {
      return nil, nil, errors.New("Syntax error: unexpected expression at "+strconv.Itoa(s_pos))
    }

    if op == "==" || op == "!=" {
      if s[s_pos] != '"' {
        for s_pos < len(s) && s[s_pos] != ' ' && s[s_pos] != '\t' && s[s_pos] != '\n' { s_pos++ }
      } else {
        quote_closed := false
        s_pos++
        for s_pos < len(s) {
          if s[s_pos] == '\\' && (s_pos+1) < len(s) && (s[s_pos+1] == '\\' || s[s_pos+1] == '"') {
            s_pos += 2
          } else if s[s_pos] == '"' {
            s_pos++
            quote_closed = true
            if s_pos < len(s) && s[s_pos] != '\n' && s[s_pos] != ' ' && s[s_pos] != '\t' {
              return nil, nil, errors.New("Syntax error: trailing symbols after quote at "+strconv.Itoa(s_pos))
            }
            break
          } else if s[s_pos] == '\n' {
            return nil, nil, errors.New("Syntax error: unclosed quote at "+strconv.Itoa(s_pos))
          } else {
            s_pos++
          }
        }
      }
    } else if op == "=~" || op == "!~" {
      if s[s_pos] != '/' {
        return nil, nil, errors.New("Syntax error: no regex opening symbol \"/\" at "+strconv.Itoa(s_pos))
      }
      s_pos++
      if len
    }
  }
}


func queue_data_sub(stop_ch chan string, wg *sync.WaitGroup) {
  //defer func() { r := recover(); if r != nil { fmt.Println("queue_data_sub: recover from:", r) } }()
  //defer func() { fmt.Println("queue_data_sub: return") }()
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
  var j []byte
  var err error

  if req.URL.Path == "/debug" || req.URL.Path == "/debug/" {
    m := make(M)
    m["data"] = data
    m["l2Matrix"] = l2Matrix
    j, err = json.MarshalIndent(m, "", "  ")
  } else if req.URL.Path == "/refs" || req.URL.Path == "/refs/" {
    j, err = json.MarshalIndent(dev_refs, "", "  ")
  } else if req.URL.Path == "/macs" {
    m := make(M)
    m["macs"]=devs_macs
    m["arp"]=devs_arp
    j, err = json.MarshalIndent(m, "", "  ")
  } else if req.URL.Path == "/" {
    j, err = json.MarshalIndent(devs, "", "  ")
  } else {
    globalMutex.RUnlock()
    http.Error(w, "Not found", http.StatusNotFound)
    return
  }
  globalMutex.RUnlock()

  if err != nil {
    http.Error(w, err.Error(), http.StatusInternalServerError)
    return
  }

  w.Header().Add("Content-Type", "text/javascript")
  w.Write(j)
  w.Write([]byte("\n"))
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
          if d, i, _err := parseGraphIntRules(redstr); err == nil {
            graph_int_rules = redstr
            graph_int_watch_dev = d
            graph_int_watch_int = i
          } else {
            if opt_v > 1 {
              color.Red("Error parsing graph_int_rules: %s", _err.Error())
            }
          }
        }
      }
    }

    if !redis_loaded && red != nil {
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
      if redis_loaded && red != nil && red.Err() == nil {
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

L466:   for dev_id, _ := range devs {
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
            if gomapper_run > 90 && (now_unix - devs.Vi(dev_id, "last_seen")) > WARN_AGE &&
                    (now_unix - dev_map.Vi(ip, "time")) > 90 && dev_map.Vs(ip, "state") == "run" {
              // if
              //var last_status = devs.Vs(dev_id, "overall_status")
              var new_status string="warn"
              if (now_unix - devs.Vi(dev_id, "last_seen")) > DEAD_AGE {
                new_status = "error"
              }

              last_alert_status, _ := devs.Vse(dev_id, "_status_alerted_value")
              if last_alert_status != new_status {

                redm := redmutex.New(fmt.Sprintf("ip_lock.%s", ip))
                err = redm.Lock(red, time.Second, 10*time.Second)
                if err != nil { continue L466 }


                var queues_map map[string]string
                queues_map, err = redis.StringMap(red.Do("HGETALL", "ip_queues."+ip))
                if err != nil {
                  redm.Unlock(red)
                  continue L466
                }

                last_error := ""

                for q, _ := range queues_map {
                  var lr string
                  lr, err = redis.String(red.Do("GET", "ip_last_result."+q+"."+ip))
                  if err != nil {
                    redm.Unlock(red)
                    continue L466
                  }

                  var res string
                  var queue_error string

                  res, _, _, queue_error, err = LastResultDecode(lr)
                  if err != nil {
                    redm.Unlock(red)
                    continue L466
                  }

                  if res != "ok" {
                    if last_error == "" {
                      last_error = queue_error
                    } else if strings.Index(last_error, queue_error) < 0 {
                      last_error += ", "+queue_error
                    }
                  }
                }
                redm.Unlock(red)

                devs.VM(dev_id)["overall_status"] = new_status
                devs.VM(dev_id)["last_error"] = last_error

                alerter := &Alerter{Conn: red}
                logger := &Logger{Conn: red, Dev: dev_id}

                logger.Event("key_change", "overall_status", "old_value", last_alert_status, "new_value", new_status)

                if alerter.Alert(devs.VM(dev_id), last_alert_status, "", "overall_status") {
                  devs.VM(dev_id)["_status_alerted_value"] = new_status
                  devs.VM(dev_id)["_status_alerted_time"] = now_unix
                }

                alerter.Save()
                logger.Save()
              }
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
