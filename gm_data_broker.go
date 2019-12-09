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
  "encoding/json"

  "github.com/gomodule/redigo/redis"
  "github.com/marcsauter/single"

  // "github.com/davecgh/go-spew/spew"

  . "github.com/ShyLionTjmn/aux"
  . "github.com/ShyLionTjmn/decode_dev"
  "github.com/ShyLionTjmn/redsub"
  //. "github.com/ShyLionTjmn/decode_dev"

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

var errInterrupted = errors.New("Interrupted")

var globalMutex = &sync.Mutex{}

var red_state_mutex = &sync.Mutex{}
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

var devs M

func process_ip_data(wg *sync.WaitGroup, ip string, paused bool) {
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

  raw, err = GetRawRed(red, ip)
  if err != nil { return }

  device := Dev{ Opt_m: true, Opt_a: true, Dev_ip: ip }

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
  if paused {
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

  if !devs.EvM(dev_id) {
    devs[dev_id] = dev
  } else {
    // check what's changed
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


  sig_ch := make(chan os.Signal, 1)
  signal.Notify(sig_ch, syscall.SIGHUP)
  signal.Notify(sig_ch, syscall.SIGINT)
  signal.Notify(sig_ch, syscall.SIGTERM)
  signal.Notify(sig_ch, syscall.SIGQUIT)

  devs = make(M)

  var wg sync.WaitGroup
  var stop_channels []chan string

  redis_loaded := false
  queue_data_sub_launched := false

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
        var wg_ sync.WaitGroup
        for ip, run := range dev_map {
          if ip_reg.MatchString(ip) {
            //fmt.Println("Load IP", ip)
chech ulimit with syscall
            wg_.Add(1)
            go process_ip_data(&wg_, ip, run != "run")
          }
        }
        wg_.Wait()
        redis_loaded = true
j, err := json.MarshalIndent(devs, "", "  ")
    if err != nil { return }
    fmt.Println(string(j))

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
