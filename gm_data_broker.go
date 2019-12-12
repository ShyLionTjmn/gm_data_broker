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
var l2Matrix = make(M) // working map with alternatives
var dev_refs = make(M) // device references for faster lookups

var opt_Q bool
var opt_1 bool
var opt_v int

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

func myHttpHandlerDebug(w http.ResponseWriter, req *http.Request) {
  req.ParseForm()
  globalMutex.RLock()
  m := make(M)
  m["data"] = data
  m["l2Matrix"] = l2Matrix
  j, err := json.MarshalIndent(m, "", "  ")
  globalMutex.RUnlock()

  if err != nil {
    http.Error(w, err.Error(), http.StatusInternalServerError)
    return
  }

  w.Header().Add("Content-Type", "text/javascript")
  w.Write(j)
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
  http.HandleFunc("/debug/", myHttpHandlerDebug)

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

    if !redis_loaded && red != nil {
      var dev_map map[string]string

      dev_map, err = redis.StringMap(red.Do("HGETALL", "dev_list"))
      if err == nil {
        total_ips := uint64(len(dev_map))
        fast_start := max_open_files > total_ips+20
        var wg_ sync.WaitGroup
        for ip, _ := range dev_map {
          if ip_reg.MatchString(ip) {
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
