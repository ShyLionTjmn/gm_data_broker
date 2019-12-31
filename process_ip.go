package main

import (
  "fmt"
  "sync"
  "time"
  "regexp"
  "errors"
  "strings"
  "strconv"
  "sort"
  "reflect"
  "encoding/json"

  "github.com/gomodule/redigo/redis"

  w "github.com/jimlawless/whereami"
  // "github.com/davecgh/go-spew/spew"
  "github.com/fatih/color"

  . "github.com/ShyLionTjmn/aux"
  . "github.com/ShyLionTjmn/decode_dev"
  . "github.com/ShyLionTjmn/gomapper_aux"

)

var safeInt_regex *regexp.Regexp

func init() {
  w.WhereAmI()
  regexp.MustCompile("")
  safeInt_regex = regexp.MustCompile(`^[a-zA-Z0-9\._\-]+$`)
}

var legNeiErrNoDev = errors.New("nd")
var legNeiErrNoIfName = errors.New("nin")
var legNeiErrNoPi = errors.New("npi")

var devWatchKeys = []string{"sysName", "locChassisSysName", "snmpEngineId", "sysLocation", "locChassisIdSubtype", "sysDescr",
                               "locChassisId", "sysObjectID", "sysContact", "CiscoConfSave", "data_ip", "short_name",
                               "powerState",
                              }

var devAlertKeys = []string{"powerState"}

var intWatchKeys = []string{"ifOperStatus", "portId", "ifAdminStatus", "ifIndex", "ifAlias", "ifType",
                            "portMode", "portTrunkVlans", "portHybridTag", "portHybridUntag", "portPvid",
                            "monitorDstSession",
                           }

var intGraphKeys = []string{"ifHCInOctets", "ifHCOutOctets", "ifInUnicastPkts", "ifOutUnicastPkts", "ifInMulticastPkts", "ifOutMulticastPkts",
                            "ifInBroadcastPkts", "ifOutBroadcastPkts", "ifOperStatus", "ifInErrors", "ifInCRCErrors",
                            "oltRxPower", "onuRxPower", "onuDistance",
                           }

var lldpKeySet = []string{"RemSysName", "RemChassisId", "RemChassisIdSubtype", "RemPortDescr", "RemPortId", "RemPortIdSubtype"}

type ByInt64 []int64

func (a ByInt64) Len() int		{ return len(a) }
func (a ByInt64) Swap(i, j int)		{ a[i], a[j] = a[j], a[i] }
func (a ByInt64) Less(i, j int) bool	{ return a[i] < a[j] }

const NL = "\n"

const LOG_MAX_EVENTS = 1000
const ALERT_MAX_EVENTS = 10000

type Logger struct {
  Conn		redis.Conn
  Dev		string
  Count		int
}

func (l *Logger) Event(f ... string) { // "event", key|"", "attr", value, "attr", value, ...
  if len(f) < 2 { return } //wtf?
  m := make(M)
  m["event"] = f[0]
  m["key"] = f[1]
  m["time"] = time.Now().Unix()

  if len(f) > 3 {
    m["fields"] = make(M)
    for i := 2; i < (len(f) - 1); i += 2 {
      m["fields"].(M)[f[i]] = f[i+1]
    }
  }

  j, err := json.Marshal(m)
  if err == nil {
    if l.Conn != nil && l.Conn.Err() == nil {
      l.Conn.Do("LPUSH", "log."+l.Dev, j)
      l.Count++
      if opt_v > 1 {
        color.Magenta("Log: %s, %s", l.Dev, j)
      }
    }
  }
}

func (l *Logger) Save() {
  if l.Conn != nil && l.Conn.Err() == nil && l.Count > 0 {
    l.Count = 0
    l.Conn.Do("LTRIM", "log."+l.Dev, 0, LOG_MAX_EVENTS)
    l.Conn.Do("PUBLISH", "log.poke", l.Dev+"\t"+time.Now().String())
  }
}

type Alerter struct {
  Conn          redis.Conn
  Count		int
}

func (a *Alerter) Alert(new M, old interface{}, ifName string, key string) (success bool) {
  success = false
  m := make(M)

  // WRITE STRINGS ONLY!
  m["id"] = new.Vs("id")
  m["data_ip"] = new.Vs("data_ip")
  m["short_name"] = new.Vs("short_name")
  m["model_short"] = new.Vs("model_short")
  m["sysObjectID"] = new.Vs("sysObjectID")
  m["sysLocation"] = new.Vs("sysLocation")
  m["last_seen"] = new.Vs("last_seen")
  m["overall_status"] = new.Vs("overall_status")
  m["alert_key"] = key
  m["time"] = strconv.FormatInt(time.Now().Unix(), 10)
  m["old"] = fmt.Sprint(old)

  for _, field := range alert_fields {
    if new.EvA(field) && !m.EvA(field) {
      m[field] = AlertRuleFieldValue(new.VA(field))
    }
  }

  if ifName == "" {
    m["alert_type"] = "dev"
    m["new"] = fmt.Sprint(new.VA(key))
  } else {
    m["alert_type"] = "int"
    for _, attr := range []string{"ifAlias", "ifType", "portMode", "ifIndex"} {
      if val, ok := new.VAe("interfaces", ifName, attr); ok {
        m[attr] = fmt.Sprint(val)
      }
    }
    m["ifName"] = ifName
    m["new"] = fmt.Sprint(new.VA("interfaces", ifName, key))
    for _, field := range alert_fields {
      if new.EvA("interfaces", ifName, field) && !m.EvA(field) {
        m[field] = AlertRuleFieldValue(new.VA("interfaces", ifName, field))
      }
    }
  }

  j, err := json.Marshal(m)
  if err == nil {
    if a.Conn != nil && a.Conn.Err() == nil {
      if _, err = a.Conn.Do("RPUSH", "alert", j); err == nil {
        a.Count++
        success = true
      }

      if opt_v > 1 {
        color.Magenta("Alert: %s", j)
      }

      if key == "overall_status" && success {
        if _, err = a.Conn.Do("SET", "status_alert."+new.Vs("id"), strconv.FormatInt(time.Now().Unix(), 10)+";"+new.Vs("data_ip")+";"+new.Vs("overall_status")); err != nil {
          success = false
        }
      }
    }
  }
  return
}

func (a *Alerter) Save() {
  if a.Conn != nil && a.Conn.Err() == nil && a.Count > 0 {
    a.Count = 0
    a.Conn.Do("LTRIM", "alert", -ALERT_MAX_EVENTS, -1)
    a.Conn.Do("PUBLISH", "alert.poke", time.Now().String())
  }
}

func leg_nei(leg M) (dev_id string, port_index string, if_name string, err error) {
  chid := leg.Vs("ChassisId")
  chidst := leg.Vi("ChassisIdSubtype")
  pid := leg.Vs("PortId")
  pidst := leg.Vi("PortIdSubtype")

  dev_id = "lldp:"+strings.ToLower(chid)
  dev_h := devs.VM(dev_id)

  if dev_h == nil {
    err = legNeiErrNoDev
  } else if dev_h.Evi("locChassisIdSubtype") && dev_h.Vi("locChassisIdSubtype") == chidst &&
     dev_h.Evs("lldp_id2port_index", pid) &&
     dev_h.Vi("lldp_ports", dev_h.Vs("lldp_id2port_index", pid), "subtype") == pidst &&
     pidst == 7 {
    //if
    port_index = dev_h.Vs("lldp_id2port_index", pid)
    if !dev_h.Evs("lldp_ports", port_index, "ifName") {
      err = legNeiErrNoIfName
    } else {
      if_name = dev_h.Vs("lldp_ports", port_index, "ifName")
    }
  } else if dev_h.Evi("locChassisIdSubtype") && dev_h.Vi("locChassisIdSubtype") == chidst &&
            pidst == 5 && dev_h.Evs("interfaces", pid, "portIndex") {
    //else if
    port_index = dev_h.Vs("interfaces", pid, "portIndex")
    if_name = pid
  } else {
    err = legNeiErrNoPi
  }
  return
}

func l2l_key(cid1, pid1, cid2, pid2 string) string {
  cid1_gt := cid1 > cid2
  cid_eq := cid1 == cid2
  pid1_gt := pid1 > pid2

  if cid1_gt || (cid_eq && pid1_gt) {
    return cid1+"@"+pid1+"#"+cid2+"@"+pid2
  } else {
    return cid2+"@"+pid2+"#"+cid1+"@"+pid1
  }
}

func wipe_dev(dev_id string) {
  delete(devs, dev_id)
  delete(devs_macs, dev_id)
  delete(devs_arp, dev_id)

  if dev_refs.EvM(dev_id, "l2_links") {
    for link_id, _ := range dev_refs.VM(dev_id, "l2_links") {
      if link_h, ok := data.VMe("l2_links", link_id); ok {
        matrix_id := link_h.Vs("matrix_id")
        alt_matrix_id := link_h.Vs("alt_matrix_id")
        var nei_leg M
        if link_h.Vs("_creator") == dev_id {
          nei_leg = link_h.VM("1")
        } else {
          nei_leg = link_h.VM("0")
        }
        if nei_dev_id, ok := nei_leg.Vse("DevId"); ok {
          nei_if := nei_leg.Vs("ifName")
          if nei_if_a, ok := devs.VAe(nei_dev_id, "interfaces", nei_if, "l2_links"); ok {
            l := len(nei_if_a.([]string))
            for i := 0; i < l; i++ {
              if nei_if_a.([]string)[i] == link_id {
                if l == 1 {
                  delete(devs.VM(nei_dev_id, "interfaces", nei_if), "l2_links")
                } else {
                  devs.VM(nei_dev_id, "interfaces", nei_if)["l2_links"] = append(nei_if_a.([]string)[:i], nei_if_a.([]string)[i+1:]...)
                }
                break
              }
            }
          }
          if dev_refs.EvM(nei_dev_id, "l2Matrix") {
            delete(dev_refs.VM(nei_dev_id, "l2Matrix"), matrix_id)
            delete(dev_refs.VM(nei_dev_id, "l2Matrix"), alt_matrix_id)
          }
          if dev_refs.EvM(nei_dev_id, "l2_links") {
            delete(dev_refs.VM(nei_dev_id, "l2_links"), link_id)
          }
        }
        delete(l2Matrix, matrix_id)
        delete(l2Matrix, alt_matrix_id)
      }
      delete(data.VM("l2_links"), link_id)
    }
  }

  if dev_refs.EvM(dev_id, "l2Matrix") {
    for matrix_id, _ := range dev_refs.VM(dev_id, "l2Matrix") {
      if matrix_h, ok := l2Matrix.VMe(matrix_id); ok {
        var nei_leg M
        if matrix_h.Vs("_creator") == dev_id {
          nei_leg = matrix_h.VM("1")
        } else {
          nei_leg = matrix_h.VM("0")
        }
        if nei_dev_id, ok := nei_leg.Vse("DevId"); ok {
          if dev_refs.EvM(nei_dev_id, "l2Matrix") {
            delete(dev_refs.VM(nei_dev_id, "l2Matrix"), matrix_id)
          }
        }
      }
      delete(l2Matrix, matrix_id)
    }
  }

  if dev_refs.EvM(dev_id, "l3_links") {
    for net, net_m := range dev_refs.VM(dev_id, "l3_links") {
      for if_ip, _ := range net_m.(M) {
        if l3link_ip_h, ok := data.VMe("l3_links", net, if_ip); ok {
          if l3link_ip_h.Vs("dev_id") == dev_id {
            delete(data.VM("l3_links", net), if_ip)
            if len(data.VM("l3_links", net)) == 0 {
              delete(data.VM("l3_links"), net)
            }
          }
        }
      }
    }
  }
  delete(dev_refs, dev_id)
}

func process_ip_data(wg *sync.WaitGroup, ip string, startup bool) {
  if wg != nil {
    defer wg.Done()
  }
  var err error
  var raw M

  ip_neighbours := 0

  var red redis.Conn

  red, err = RedisCheck(red, "unix", REDIS_SOCKET, red_db)

  if red == nil {
    if opt_v > 1 { color.Red("%s", err.Error()) }
    return
  }

  defer func() { if red != nil { red.Close() } }()

  defer func() {
    if err != nil {
      if red != nil && red.Err() == nil {
        ip_err := fmt.Sprintf("%d:%s ! %s", time.Now().Unix(), time.Now().Format("2006 Jan 2 15:04:05"), err.Error())
        red.Do("SET", "ip_proc_error."+ip, ip_err)
      }

      globalMutex.Lock()
      if data.EvM("dev_list", ip) {
        data.VM("dev_list", ip)["proc_error"] = err.Error()
        data.VM("dev_list", ip)["proc_result"] = "error"
        data.VM("dev_list", ip)["time"] = time.Now().Unix()
      }
      globalMutex.Unlock()
      if opt_v > 1 { color.Red("%s", err.Error()) }
    }
  }()

  var dev_list_state string
  var dev_list_state_str string
  dev_list_state_str, err = redis.String(red.Do("HGET", "dev_list", ip))
  if err == nil {
    err = redis.ErrNil
    a := strings.Split(dev_list_state_str, ":")
    if len(a) == 2 && a[1] != "ignore" {
      t, _err := strconv.ParseInt(a[0], 10, 64)
      if _err == nil && t <= time.Now().Unix() {
        dev_list_state = a[1]
        err = nil
      }
    }
  }
  if err != nil {
    if err == redis.ErrNil && !startup {
      //device removed from dev_list, just cleanup and return
      err = nil

      globalMutex.Lock()
      defer globalMutex.Unlock()

      //remove from dev_list
      delete(data.VM("dev_list"), ip)

      //find dev id by data_ip

      log := &Logger{Conn: red, Dev: "nodev"}

      for dev_id, dev_m := range devs {
        if dev_m.(M).Vs("data_ip") == ip {
          log.Event("dev_purged", "", "ip", ip)
          wipe_dev(dev_id)
          if opt_v > 0 {
            color.Yellow("Dev purged: %s, %s", dev_id, ip)
          }
        }
      }

      log.Save()

      if opt_v > 1 {
        color.Yellow("Dev gone: %s", ip)
      }
    }
    return
  }

  if dev_list_state == "conflict" {
    if !startup {
      //dying gasp from gomapper
      //all states should have been set before
    } else {
      //get proc error from redis
      errstr, _err := redis.String(red.Do("GET", "ip_proc_error."+ip))
      if _err != nil {
        errstr = "Unknown proc error"
      }
      globalMutex.Lock()
      dl_h := data.MkM("dev_list", ip)
      dl_h["proc_result"] = "error"
      dl_h["proc_error"] = errstr
      dl_h["state"] = dev_list_state
      dl_h["time"] = time.Now().Unix()
      globalMutex.Unlock()
    }
    return
  }

  globalMutex.Lock()
  dl_h := data.MkM("dev_list", ip)
  dl_h["proc_result"] = "in-progress"
  dl_h["proc_error"] = ""

  prev_dev_list_state := dl_h.Vs("state")

  dl_h["state"] = dev_list_state
  dl_h["time"] = time.Now().Unix()
  globalMutex.Unlock()

  raw, err = GetRawRed(red, ip)
  if err != nil {
    if !startup && prev_dev_list_state != "run" && dev_list_state == "run" && err == ErrorQueuesMismatch {
      //ignore freshly started device with many queues - not all of them saved yet
      err = nil
      globalMutex.Lock()
      defer globalMutex.Unlock()
      data.VM("dev_list", ip)["proc_result"] = "postproned"
      if opt_v > 1 {
        fmt.Println("Postprone:", ip)
      }
    }

    if startup && err == redis.ErrNil {
      err = nil
    }

    return
  }

  device := Dev{ Opt_m: true, Opt_a: true, Dev_ip: ip }

  process_start := time.Now()

  err = device.Decode(raw)
  if err != nil {
    if startup && err == redis.ErrNil {
      err = nil
    }
    return
  }

  now_unix := time.Now().Unix()
  now_unix_str := strconv.FormatInt(now_unix, 10)

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

  if opt_v > 1 {
    fmt.Println("Process:", ip)
  }

  save_time := int64(0)
  last_error := ""
  overall_status := "ok"

  if dev_list_state != "run" {
    overall_status = "paused"
  }

  has_errors := false

  for _, q := range queue_list.([]string) {
    if !dev.Evs("_last_result", q) {
      err = errors.New("No _last_result for queue "+q)
      return
    }

    lr := dev.Vs("_last_result", q)

    var res string
    var queue_save_time int64
    var queue_error string

    res, _, queue_save_time, queue_error, err = LastResultDecode(lr)
    if err != nil { return }
    if res != "ok" {
      has_errors = true
      if overall_status == "ok" {
        overall_status = "error"
      }
      if last_error == "" {
        last_error = queue_error
      } else if strings.Index(last_error, queue_error) < 0 {
        last_error += ", "+queue_error
      }
    } else {
      if queue_save_time > save_time {
        save_time = queue_save_time
      }
    }
  }

  if has_errors {
    save_time = 0
  }

/*
  if (time.Now().Unix() - last_seen) > WARN_AGE && overall_status == "ok" {
    overall_status = "warn"
  }
*/
  dev["overall_status"] = overall_status
  dev["last_error"] = last_error
  dev["save_time"] = save_time

  dev_id := dev.Vs("id")

  var redstr string
  redstr, err = redis.String(red.Do("GET", "status_alert."+dev_id))

  if err != nil && err != redis.ErrNil {
    return
  }

  status_alerted_value := ""
  status_alerted_time := int64(0)

  if err == nil {
    a := strings.Split(redstr, ";")
    if len(a) == 3 && a[1] == ip {
      status_alerted_time, err = strconv.ParseInt(a[0], 10, 64)
      if err != nil { return }
      status_alerted_value = a[2]
    }
  }

  err = nil

  globalMutex.Lock()
  defer globalMutex.Unlock()

  if devs.EvM(dev_id) && devs.Vs(dev_id, "data_ip") != ip {
    logger := &Logger{Conn: red, Dev: "nodev"}
    if opt_v > 0 {
      color.Red("CONFLICT: %s vs %s", devs.Vs(dev_id, "data_ip"), ip)
    }
    conflict_ip := devs.Vs(dev_id, "data_ip")
    //there is duplicate device id
    if overall_status == "ok" && devs.Vs(dev_id, "overall_status") != "ok" {
      // duplicate device is old, overwrite it
      wipe_dev(dev_id)
      ip_err := fmt.Sprintf("%d:%s ! %s", time.Now().Unix(), time.Now().Format("2006 Jan 2 15:04:05"), "Pausing due to conflict with running device "+ip)
      red.Do("SET", "ip_proc_error."+conflict_ip, ip_err)
      red.Do("HSET", "dev_list", conflict_ip, now_unix_str+":conflict")

      logger.Event("conflict", "", "conflict_id", dev_id, "paused_ip", conflict_ip, "working_ip", ip)
      logger.Save()

    } else if devs.Vs(dev_id, "overall_status") == "ok" && overall_status != "ok" {
      // this device is old or paused, ignore data
      red.Do("HSET", "dev_list", ip, now_unix_str+":conflict")
      err = errors.New("Conflict with running dev "+conflict_ip+". Pausing. Prev status was: "+overall_status)
      data.VM("dev_list", ip)["state"] = "conflict"
      logger.Event("conflict", "", "conflict_id", dev_id, "paused_ip", ip, "working_ip", conflict_ip)
      logger.Save()
      return
    } else {
      //both good or both bad. compare save_time
      if save_time > devs.Vi(dev_id, "save_time") {
        //this dev is more recent
        wipe_dev(dev_id)
        ip_err := fmt.Sprintf("%d:%s ! %s", time.Now().Unix(), time.Now().Format("2006 Jan 2 15:04:05"), "Pausing due to conflict with more recent device "+ip)
        red.Do("SET", "ip_proc_error."+conflict_ip, ip_err)
        red.Do("HSET", "dev_list", conflict_ip, now_unix_str+":conflict")
        logger.Event("conflict", "", "conflict_id", dev_id, "paused_ip", conflict_ip, "working_ip", ip)
        logger.Save()
      } else {
        //this dev data is older
        red.Do("HSET", "dev_list", ip, now_unix_str+":conflict")
        err = errors.New("Conflict with more recent dev "+conflict_ip+". Pausing. Prev status was: "+overall_status)
        data.VM("dev_list", ip)["state"] = "conflict"
        logger.Event("conflict", "", "conflict_id", dev_id, "paused_ip", ip, "working_ip", conflict_ip)
        logger.Save()
        return
      }
    }
  }

  var last_seen int64

  //check for id change
  if prev_id, ok := data.Vse("dev_list", ip , "id"); ok && prev_id != dev_id {
    wipe_dev(prev_id)
    logger := &Logger{Conn: red, Dev: "nodev"}
    logger.Event("dev_id_change", "", "prev_id", prev_id, "new_id", dev_id, "ip", ip)
    logger.Save()
    if opt_v > 0 {
      color.Yellow("Dev id changed. Previous data purged: %s, %s", prev_id, ip)
    }
  } else {
    data.VM("dev_list", ip)["id"] = dev_id

    var redstr string
    redstr, err = redis.String(red.Do("GET", "dev_last_seen."+dev_id))
    if err != nil && err != redis.ErrNil { return }
    if err == nil {
      i, s, _err := IntSepStrErr(redstr, ":")
      if _err == nil && s == ip {
        last_seen = i
      }
    }
  }

  if last_seen < save_time {
    last_seen = save_time
  }

  if overall_status == "error" && (now_unix - last_seen) < DEAD_AGE {
    overall_status = "warn"
    dev["overall_status"] = overall_status
  }

  dev["last_seen"] = last_seen
  // process links
  check_matrix := make(M)

  if sysoids_h, ok := data.VMe("sysoids", dev.Vs("sysObjectID")); ok {
    dev["model_short"] = sysoids_h.Vs("short")
    dev["model_long"] = sysoids_h.Vs("long")
  } else {
    dev["model_short"] = "Unknown"
    dev["model_long"] = "Unknown"
  }

  // build l2 neighbour matrix
  if dev.EvM("lldp_ports") && dev.Evs("locChassisId") {
    for port_index, port_h := range dev.VM("lldp_ports") {
      if port_h.(M).EvM("neighbours") {
        for _, nei_h := range port_h.(M).VM("neighbours") {
          if rem_ip, ok := nei_h.(M).Vse("RemMgmtAddr", "1"); ok {
            if ifName, ok := port_h.(M).Vse("ifName"); ok {
              if !dev.EvA("interfaces", ifName, "ip_neighbours") {
                dev.VM("interfaces", ifName)["ip_neighbours"] = make([]string, 0)
              }
              dev.VM("interfaces", ifName)["ip_neighbours"] = StrAppendOnce(dev.VA("interfaces", ifName, "ip_neighbours").([]string), rem_ip)
              ip_neighbours++
            }
          }
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

          var matrix_h M
          var pass = 0

          if l2Matrix.EvM(matrix_id) {
            matrix_h = l2Matrix.VM(matrix_id)
            if matrix_h.Vs("_creator") != dev_id {

              prev_complete := matrix_h.Vi("_complete")
              //reset its status
              matrix_h["status"] = int64(2)
              matrix_h["_complete"] = int64(0)

              // we did not create it
              leg1_h := matrix_h.VM("1")
              leg1_h["PortIndex"] = port_index
              leg1_h["DevId"] = dev_id

              dev_refs.MkM(dev_id, "l2Matrix", matrix_id)

              matrix_h["link_id"] = l2l_key(matrix_h.Vs("0", "DevId"), matrix_h.Vs("0", "PortIndex"), matrix_h.Vs("1", "DevId"), matrix_h.Vs("1", "PortIndex"))

              if port_h.(M).Evs("ifName") {
                leg1_h["ifName"] = port_h.(M).Vs("ifName")
                if matrix_h.Evs("0", "ifName") {
                  rifname := matrix_h.Vs("0", "ifName")
                  rdevid := matrix_h.Vs("0", "DevId")

                  if devs.EvM(rdevid, "interfaces", rifname) &&
                     matrix_h.Vi("_time") == devs.Vi(rdevid, "last_seen") {
                    // if
                    matrix_h["_complete"] = int64(1)
                    matrix_h["status"] = int64(1)
                    check_matrix[matrix_id] = rdevid
                    if opt_l {
                      if prev_complete == 1 {
                        color.HiBlack("link updateed by meeting: %s", matrix_h.Vs("link_id"))
                      } else {
                        color.Green("link established by meeting: %s", matrix_h.Vs("link_id"))
                      }
                    }
                  }
                }
              }

              continue
            } else {
              pass++
            }

          } else {
            matrix_h = l2Matrix.MkM(norm_matrix_id)
            dev_refs.MkM(dev_id, "l2Matrix", norm_matrix_id)
          }

          matrix_h["_creator"] = dev_id
          matrix_h["_complete"] = int64(0)
          matrix_h["alt"] = int64(0)
          matrix_h["status"] = int64(2)
          matrix_h["matrix_id"] = norm_matrix_id
          matrix_h["alt_matrix_id"] = alt_matrix_id
          matrix_h["_time"] = last_seen

          leg0_h := matrix_h.MkM("0")
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

          leg1_h := matrix_h.MkM("1")
          leg1_h["ChassisId"] = rcid
          leg1_h["ChassisIdSubtype"] = nei_h.(M).VA("RemChassisIdSubtype")
          leg1_h["PortId"] = rport_id
          leg1_h["PortIdSubtype"] = nei_h.(M).VA("RemPortIdSubtype")
          leg1_h["ChassisSysName"] = nei_h.(M).VA("RemSysName")

          nei_dev_id, nei_port_index, nei_ifname, nei_error := leg_nei(leg1_h)

          if nei_error != legNeiErrNoDev && nei_error != legNeiErrNoPi {
            leg1_h["DevId"] = nei_dev_id
            dev_refs.MkM(nei_dev_id, "l2Matrix", norm_matrix_id)
            leg1_h["PortIndex"] = nei_port_index
            matrix_h["link_id"] = l2l_key(matrix_h.Vs("0", "DevId"), matrix_h.Vs("0", "PortIndex"), matrix_h.Vs("1", "DevId"), matrix_h.Vs("1", "PortIndex"))
            if nei_error != legNeiErrNoIfName {
              leg1_h["ifName"] = nei_ifname
              if port_h.(M).Evs("ifName") {
                matrix_h["status"] = int64(1)
                matrix_h["_complete"] = int64(1)
                check_matrix[norm_matrix_id] = dev_id
                if opt_l {
                  if pass == 0 {
                    color.Green("link established by creator: %s", matrix_h.Vs("link_id"))
                  } else {
                    color.HiBlack("link updated by creator: %s", matrix_h.Vs("link_id"))
                  }
                }
              }
            }
          }

          if norm_matrix_id != alt_matrix_id && matrix_h["_complete"] != 1 {
            alt_matrix_h := matrix_h.Copy()
            alt_matrix_h["alt"] = int64(1)
            alt_matrix_h["alt_matrix_id"] = norm_matrix_id
            alt_matrix_h["matrix_id"] = alt_matrix_id
            l2Matrix[alt_matrix_id] = alt_matrix_h
            dev_refs.MkM(dev_id, "l2Matrix", alt_matrix_id)
            if alt_matrix_h.Evs("1", "Dev_id") {
              dev_refs.MkM(alt_matrix_h.Vs("1", "Dev_id"), "l2Matrix", alt_matrix_id)
            }
          }
        }
      }
    }
  }

  //copy links from previous run and cleanup outdated
  //if !startup && devs.EvM(dev_id, "interfaces") 
  if !startup && devs.EvM(dev_id) {
    for ifName, if_h := range devs.VM(dev_id, "interfaces") {
      if if_h.(M).EvA("l2_links") && dev.EvM("interfaces", ifName) {
        link_list := make([]string, 0)
        for _, link_id := range if_h.(M).VA("l2_links").([]string) {
          keep_link := false
          link_h := data.VM("l2_links", link_id)
          went_down := false

          if link_h != nil {
            matrix_id := link_h.Vs("matrix_id")
            if l2Matrix.EvM(matrix_id) {
              matrix_h := l2Matrix.VM(matrix_id)
              creator := matrix_h.Vs("_creator")
              l0_ifName := matrix_h.Vs("0", "ifName")
              l1_ifName := matrix_h.Vs("1", "ifName")
              l1_devId := matrix_h.Vs("1", "DevId")
              if (creator == dev_id && devs.EvA(l1_devId, "interfaces", l1_ifName, "l2_links")) ||
                 (creator != dev_id && devs.EvA(creator, "interfaces", l0_ifName, "l2_links")) {
                //if
                if (creator == dev_id && matrix_h.Vi("_time") == last_seen && check_matrix.Evs(matrix_id)) ||
                   (creator != dev_id && matrix_h.Vi("_time") == devs.Vi(creator, "last_seen")) ||
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
                            devs.Vi(l1_devId, "interfaces", l1_ifName, "ifOperStatus") == 2 ||
                            false)) ||
                          (creator != dev_id &&
                           len(if_h.(M).VA("l2_links").([]string)) == 1 &&
                           len(devs.VA(creator, "interfaces", l0_ifName, "l2_links").([]string)) == 1 &&
                           (dev.Vs("overall_status") != "ok" ||
                            dev.Vi("interfaces", l0_ifName, "ifOperStatus") == 2 ||
                            devs.Vs(creator, "overall_status") != "ok" ||
                            devs.Vi(creator, "interfaces", l0_ifName, "ifOperStatus") == 2 ||
                            false)) ||
                          false {
                  //else if
                  keep_link = true
                  if link_h.Vi("status") != 2 {
                    went_down = true
                  }
                  link_h["status"] = int64(2)
                  matrix_h["status"] = int64(2)
                }

              }
            }
          }
          if keep_link {
            link_list = append(link_list, link_id)
            if went_down {
              if opt_l {
                color.Magenta("link down: %s", link_id)
              }
            }
          } else {
            if opt_l {
              color.Cyan("link gone: %s", link_id)
            }
            if link_h != nil {
              creator := link_h.Vs("_creator")
              l0_ifName := link_h.Vs("0", "ifName")
              l1_ifName := link_h.Vs("1", "ifName")
              l1_devId := link_h.Vs("1", "DevId")
              matrix_id := link_h.Vs("matrix_id")
              alt_matrix_id := link_h.Vs("alt_matrix_id")

              if creator == dev_id && devs.EvA(l1_devId, "interfaces", l1_ifName, "l2_links") {
                list := devs.VA(l1_devId, "interfaces", l1_ifName, "l2_links").([]string)
                found := -1
                for i := 0; i < len(list); i++ { if list[i] == link_id { found = i; break } }
                if found >= 0 {
                  if len(list) > 1 {
                    devs.VM(l1_devId, "interfaces", l1_ifName)["l2_links"] = append(list[:found], list[found+1:]...)
                  } else {
                    delete(devs.VM(l1_devId, "interfaces", l1_ifName), "l2_links")
                  }
                }
              }

              if creator != dev_id && devs.EvA(creator, "interfaces", l0_ifName, "l2_links") {
                list := devs.VA(creator, "interfaces", l0_ifName, "l2_links").([]string)
                found := -1
                for i := 0; i < len(list); i++ { if list[i] == link_id { found = i; break } }
                if found >= 0 {
                  if len(list) > 1 {
                    devs.VM(creator, "interfaces", l0_ifName)["l2_links"] = append(list[:found], list[found+1:]...)
                  } else {
                    delete(devs.VM(creator, "interfaces", l0_ifName), "l2_links")
                  }
                }
              }


              if dev_refs.EvM(creator, "l2Matrix") {
                delete(dev_refs.VM(creator, "l2Matrix"), matrix_id)
                delete(dev_refs.VM(creator, "l2Matrix"), alt_matrix_id)
              }
              if dev_refs.EvM(creator, "l2_links") { delete(dev_refs.VM(creator, "l2_links"), link_id) }
              if creator == dev_id {
                if dev_refs.EvM(l1_devId, "l2Matrix") {
                  delete(dev_refs.VM(l1_devId, "l2Matrix"), matrix_id)
                  delete(dev_refs.VM(l1_devId, "l2Matrix"), alt_matrix_id)
                }
                if dev_refs.EvM(l1_devId, "l2_links") { delete(dev_refs.VM(l1_devId, "l2_links"), link_id) }
              } else {
                if dev_refs.EvM(dev_id, "l2Matrix") {
                  delete(dev_refs.VM(dev_id, "l2Matrix"), matrix_id)
                  delete(dev_refs.VM(dev_id, "l2Matrix"), alt_matrix_id)
                }
                if dev_refs.EvM(dev_id, "l2_links") { delete(dev_refs.VM(dev_id, "l2_links"), link_id) }
              }

              delete(data.VM("l2_links"), link_id)

              delete(l2Matrix, matrix_id)
              delete(l2Matrix, alt_matrix_id)

              delete(check_matrix, matrix_id)
              delete(check_matrix, alt_matrix_id)

            }
          }
        }

        if len(link_list) > 0 {
          dev.VM("interfaces", ifName)["l2_links"] = link_list
        }
      }
    }
  }


  for matrix_id, _ := range check_matrix {
    var if0_h M
    var if1_h M
    matrix_h, ex := l2Matrix.VMe(matrix_id)
    link_id := matrix_h.Vs("link_id")

    dev_refs.MkM(dev_id, "l2_links", link_id)

    if check_matrix.Vs(matrix_id) == dev_id {
      if0_h = dev.VM("interfaces", matrix_h.Vs("0", "ifName"))
      if1_h = devs.VM(matrix_h.Vs("1", "DevId"), "interfaces", matrix_h.Vs("1", "ifName"))

      dev_refs.MkM(matrix_h.Vs("1", "DevId"), "l2_links", link_id)
    } else {
      if0_h = devs.VM(matrix_h.Vs("0", "DevId"), "interfaces", matrix_h.Vs("0", "ifName"))
      if1_h = dev.VM("interfaces", matrix_h.Vs("1", "ifName"))
      dev_refs.MkM(matrix_h.Vs("0", "DevId"), "l2_links", link_id)
    }

    if if0_h == nil {
      fmt.Println("dev_id", dev_id)
      fmt.Println("matrix_id", matrix_id)
      fmt.Println("check_matrix.Vs(matrix_id)", check_matrix.Vs(matrix_id))
      fmt.Println(ex)
      fmt.Println(matrix_h)
panic("Boo")
    }

    if !if0_h.EvA("l2_links") {
      if0_h["l2_links"] = make([]string, 0)
    }

    found := false

    for _, l_id := range if0_h["l2_links"].([]string) {
      if l_id == link_id {
        found = true
        break
      }
    }

    if !found {
      if0_h["l2_links"] = append(if0_h["l2_links"].([]string), link_id)
    }

    if !if1_h.EvA("l2_links") {
      if1_h["l2_links"] = make([]string, 0)
    }

    found = false

    for _, l_id := range if1_h["l2_links"].([]string) {
      if l_id == link_id {
        found = true
        break
      }
    }

    if !found {
      if1_h["l2_links"] = append(if1_h["l2_links"].([]string), link_id)
    }

    if !data.EvM("l2_links", link_id) {
      data.VM("l2_links")[link_id] = l2Matrix.VM(matrix_id)
    }
  }


  if dev.EvM("interfaces") {
    for ifName, if_m := range dev.VM("interfaces") {
      if ips, ok := if_m.(M).VMe("ips"); ok {
        for if_ip, if_ip_m := range ips {
          if net, ok := if_ip_m.(M).Vse("net"); ok {
            register := false
            if l3link_ip_h, ok := data.VMe("l3_links", net, if_ip); ok {
              link_dev_id := l3link_ip_h.Vs("dev_id")
              if link_dev_id != dev_id || l3link_ip_h.Vs("ifName") != ifName {
                if startup {
                  color.Red("IP conflict: %s, %s vs %s", if_ip, dev_id, link_dev_id)
                } else {
                  if devs.EvM(link_dev_id) && devs.Vs(link_dev_id, "overall_status") == "ok" &&
                     (link_dev_id != dev_id || l3link_ip_h.Vi("time") == now_unix) {
                    //if
                    if opt_v > 1 {
                      color.Red("IP conflict: %s, %s vs %s", if_ip, dev_id, link_dev_id)
                    }
                  } else {
                    //overwrite data
                    if dev_refs.EvM(link_dev_id, "l3_links", net) {
                      delete(dev_refs.VM(link_dev_id, "l3_links", net), if_ip)
                      if len(dev_refs.VM(link_dev_id, "l3_links", net)) == 0 {
                        delete(dev_refs.VM(link_dev_id, "l3_links"), net)
                      }
                      if len(dev_refs.VM(link_dev_id, "l3_links")) == 0 {
                        delete(dev_refs.VM(link_dev_id), "l3_links")
                      }
                    }
                    register = true
                  }
                }
              } else {
                register = true
              }
            } else {
              register = true
            }
            if register {
              l3link_ip_h := data.MkM("l3_links", net, if_ip)
              l3link_ip_h["dev_id"] = dev_id
              l3link_ip_h["ifName"] = ifName
              l3link_ip_h["time"] = now_unix
              dev_refs.MkM(dev_id, "l3_links", net, if_ip)
            }
          }
        }
      }
    }
  }

  if device.Opt_m && device.Dev_macs != nil && len(device.Dev_macs) > 0 {
    devs_macs[dev_id] = device.Dev_macs
  } else {
    delete(devs_macs, dev_id)
  }

  if device.Opt_a && device.Dev_arp != nil && len(device.Dev_arp) > 0 {
    devs_arp[dev_id] = device.Dev_arp
  } else {
    delete(devs_arp, dev_id)
  }

  dev["_startup"] = startup

  esc_dev_id := SafeDevId(dev_id)
  //esc_dev_id := dev.Vs("short_name")

  if (startup || !devs.EvM(dev_id)) && red != nil && red.Err() == nil && graph_int_rules_time > 0 {
    //create graph items

    red_args := redis.Args{}.Add("ip_graphs."+ip)
    red_args = red_args.Add("time", time.Now().Unix())

    if dev.EvM("CPUs") {
      for cpu_id, _ := range dev.VM("CPUs") {
        if gk, ok := dev.Vse("CPUs", cpu_id, "_graph_key"); ok {
          gf := esc_dev_id+"/CPU."+gk+".rrd"
          red_args = red_args.Add(gk, gf)
          dev.VM("CPUs", cpu_id)["_graph_file"] = gf
        }
      }
    }

    if dev.EvA("memoryUsed") {
      gf := esc_dev_id+"/memoryUsed.rrd"
      red_args = red_args.Add("memoryUsed.0", gf)
      dev["memoryUsed_graph_file"] = gf
    }

    if dev.EvM("interfaces") {
      for ifName, int_m := range dev.VM("interfaces") {
        int_h := int_m.(M)
        ifIndex := int_h.Vs("ifIndex")
        esc_if_name := SafeIntId(ifName)
        if ok, _ := MatchGraphIntRules(graph_int_rules, dev, ifName); ok && safeInt_regex.MatchString(esc_if_name) {
          gf_prefix := esc_dev_id+"/"+esc_if_name
          int_h["_graph_prefix"] = gf_prefix
          for _, key := range intGraphKeys {
            if int_h.EvA(key) {
              gf := gf_prefix+"."+key+".rrd"
              red_args = red_args.Add(key+"."+ifIndex, gf)
            }
          }
        }
      }
    }

    red.Send("MULTI")
    red.Send("DEL", "ip_graphs."+ip)
    red.Send("HSET", red_args...)
    red.Do("EXEC")

    dev["_graph_int_rules_time"] = graph_int_rules_time
  }

  if graph_int_rules_time == 0 {
    dev["_graph_int_rules_time"] =  int64(0)
  }

  reg_ip_dev_id := false

  if startup {
    dev["_status_alerted_value"] = status_alerted_value
    dev["_status_alerted_time"] = status_alerted_time
    devs[dev_id] = dev
    reg_ip_dev_id = true
  } else {
    if old, ok := devs.VMe(dev_id); !ok {
      logger := &Logger{Conn: red, Dev: "nodev"}
      devs[dev_id] = dev
      location, _ := dev.Vse("sysLocation")
      logger.Event("dev_new", "", "ip", ip, "short_name", dev.Vs("short_name"), "loc", location)
      logger.Save()
      reg_ip_dev_id = true
    } else {
      dev["_graph_int_rules_time"] = old.Vi("_graph_int_rules_time")

      reevaluate := dev.Vi("_graph_int_rules_time") != graph_int_rules_time

      graphChanges := make([]string, 0)
      if opt_v > 1 && old.Vi("_graph_int_rules_time") != graph_int_rules_time {
        fmt.Println("graph_int_rules_time changed", old.Vi("_graph_int_rules_time"), " -> ", graph_int_rules_time)
      }

      if graph_int_rules_time > 0 && !reevaluate {
        //check if rules fields had changed
        for _, key := range graph_int_watch_dev {
          if (dev.EvA(key) && !old.EvA(key)) ||
             (!dev.EvA(key) && old.EvA(key)) ||
             (dev.EvA(key) && old.EvA(key) && dev.VA(key) != old.VA(key)) ||
             (dev.EvA(key) && old.EvA(key) && reflect.TypeOf(dev.VA(key)) != reflect.TypeOf(old.VA(key))) ||
             false {
            //if
            reevaluate = true
            graphChanges = append(graphChanges, key)
            break
          }
        }
      }

      if graph_int_rules_time > 0 && !reevaluate {
        //check if rules fields had changed
        for _, key := range graph_int_watch_dev_ne {
          if (dev.EvA(key) && !old.EvA(key)) ||
             (!dev.EvA(key) && old.EvA(key)) ||
             (dev.EvA(key) && old.EvA(key) && reflect.TypeOf(dev.VA(key)) != reflect.TypeOf(old.VA(key))) ||
             false {
            //if
            reevaluate = true
            graphChanges = append(graphChanges, key)
            break
          } else if dev.EvA(key) && old.EvA(key) {
            len_new := 0
            len_old := 0
            //if
            switch dev.VA(key).(type) {
            case []interface{}:
              len_new = len(dev.VA(key).([]interface{}))
              len_old = len(old.VA(key).([]interface{}))
            case []string:
              len_new = len(dev.VA(key).([]string))
              len_old = len(old.VA(key).([]string))
            case []int64:
              len_new = len(dev.VA(key).([]int64))
              len_old = len(old.VA(key).([]int64))
            case []uint64:
              len_new = len(dev.VA(key).([]uint64))
              len_old = len(old.VA(key).([]uint64))
            }
            if len_new != len_old {
              reevaluate = true
              graphChanges = append(graphChanges, key)
              break
            }
          } else if (dev.EvM(key) && !old.EvM(key)) ||
             (!dev.EvM(key) && old.EvM(key)) ||
             (dev.EvM(key) && old.EvM(key) && len(dev.VM(key)) != len(old.VM(key))) ||
             false {
            //if
            graphChanges = append(graphChanges, key)
            reevaluate = true
            break
          }
        }
      }

      logger := &Logger{Conn: red, Dev: dev_id}
      alerter := &Alerter{Conn: red}

      // check what's changed
//fmt.Println(w.WhereAmI(), dev.Vs("overall_status"))
      if status_alerted_value != dev.Vs("overall_status") {
        if alerter.Alert(dev, status_alerted_value, "", "overall_status") {
          status_alerted_value = dev.Vs("overall_status")
          status_alerted_time = time.Now().Unix()
        }
      }

      if old.Vs("overall_status") != dev.Vs("overall_status") {
        logger.Event("key_change", "overall_status", "old_value", old.Vs("overall_status"), "new_value", dev.Vs("overall_status"))
      }

      for _, key := range devWatchKeys {
        if old.EvA(key) && !dev.EvA(key) {
          logger.Event("key_gone", key, "old_value", old.Vs(key))
        } else if !old.EvA(key) && dev.EvA(key) {
          logger.Event("key_new", key, "new_value", dev.Vs(key))
        } else if old.EvA(key) && dev.EvA(key) && reflect.TypeOf(old.VA(key)) != reflect.TypeOf(dev.VA(key)) {
          logger.Event("key_type_change", key, "old_type", reflect.TypeOf(old.VA(key)).String(), "new_type", reflect.TypeOf(dev.VA(key)).String())
          logger.Event("key_change", key, "old_value", old.Vs(key), "new_value", dev.Vs(key))
        } else if old.EvA(key) && dev.EvA(key) && old.VA(key) != dev.VA(key) {
          logger.Event("key_change", key, "old_value", old.Vs(key), "new_value", dev.Vs(key))

          if IndexOf(devAlertKeys, key) >= 0 {
            //alert if changes
            alerter.Alert(dev, old.VA(key), "", key)
          }
        }
      }

      for ifName, _ := range dev.VM("interfaces") {
        if !old.EvM("interfaces", ifName) {
          logger.Event("if_new", ifName)
        } else {
          if ifName != "CPU port" {

            if graph_int_rules_time > 0 && !reevaluate {
              if dev.EvA("interfaces", ifName, "ifIndex") != old.EvA("interfaces", ifName, "ifIndex") {
                reevaluate = true
                graphChanges = append(graphChanges, ifName+":ifIndex")
              }
            }

            if graph_int_rules_time > 0 && !reevaluate {

              //check if rules fields had changed
              for _, key := range graph_int_watch_int {
                if (dev.EvA("interfaces", ifName, key) && !old.EvA("interfaces", ifName, key)) ||
                   (!dev.EvA("interfaces", ifName, key) && old.EvA("interfaces", ifName, key)) ||
                   (dev.EvA("interfaces", ifName, key) && old.EvA("interfaces", ifName, key) &&
                    reflect.TypeOf(dev.VA("interfaces", ifName, key)) != reflect.TypeOf(old.VA("interfaces", ifName, key))) ||
                   (dev.EvA("interfaces", ifName, key) && old.EvA("interfaces", ifName, key) &&
                    dev.VA("interfaces", ifName, key) != old.VA("interfaces", ifName, key)) ||
                   false {
                  //if
                  reevaluate = true
                  graphChanges = append(graphChanges, ifName+":"+key)
                  break
                }
              }
            }

            if graph_int_rules_time > 0 && !reevaluate {

              //check if rules fields had changed
              for _, key := range graph_int_watch_int_ne {
                if (dev.EvA("interfaces", ifName, key) && !old.EvA("interfaces", ifName, key)) ||
                   (!dev.EvA("interfaces", ifName, key) && old.EvA("interfaces", ifName, key)) ||
                   (dev.EvA("interfaces", ifName, key) && old.EvA("interfaces", ifName, key) &&
                    reflect.TypeOf(dev.VA("interfaces", ifName, key)) != reflect.TypeOf(old.VA("interfaces", ifName, key))) ||
                   false {
                  //if
                  graphChanges = append(graphChanges, ifName+":"+key)
                  reevaluate = true
                  break
                } else if dev.EvA("interfaces", ifName, key) && old.EvA("interfaces", ifName, key) {
                  len_new := 0
                  len_old := 0
                  //if
                  switch dev.VA("interfaces", ifName, key).(type) {
                  case []interface{}:
                    len_new = len(dev.VA("interfaces", ifName, key).([]interface{}))
                    len_old = len(old.VA("interfaces", ifName, key).([]interface{}))
                  case []string:
                    len_new = len(dev.VA("interfaces", ifName, key).([]string))
                    len_old = len(old.VA("interfaces", ifName, key).([]string))
                  case []int64:
                    len_new = len(dev.VA("interfaces", ifName, key).([]int64))
                    len_old = len(old.VA("interfaces", ifName, key).([]int64))
                  case []uint64:
                    len_new = len(dev.VA("interfaces", ifName, key).([]uint64))
                    len_old = len(old.VA("interfaces", ifName, key).([]uint64))
                  }
                  if len_new != len_old {
                    graphChanges = append(graphChanges, ifName+":"+key)
                    reevaluate = true
                    break
                  }
                } else if (dev.EvM("interfaces", ifName, key) && !old.EvM("interfaces", ifName, key)) ||
                   (!dev.EvM("interfaces", ifName, key) && old.EvM("interfaces", ifName, key)) ||
                   (dev.EvM("interfaces", ifName, key) && old.EvM("interfaces", ifName, key) &&
                    len(dev.VM("interfaces", ifName, key)) != len(old.VM("interfaces", ifName, key))) ||
                   false {
                  //if
                  graphChanges = append(graphChanges, ifName+":"+key)
                  reevaluate = true
                  break
                }
              }
            }

//debug_printed := false
            for _, key := range intWatchKeys {
              if !old.EvA("interfaces", ifName, key) && dev.EvA("interfaces", ifName, key) {
                logger.Event("if_key_new", ifName, "key", key)
              } else if old.EvA("interfaces", ifName, key) && !dev.EvA("interfaces", ifName, key) {
/*
if key == "ifOperStatus" && !debug_printed {
  fmt.Println(ip)
  fmt.Println(raw.VM("ifOperStatus"))
  debug_printed = true
  red.Do("HSET", "dev_list", ip, now_unix_str+":debug")
  panic("ifOperStatus")
}
*/
                logger.Event("if_key_gone", ifName, "key", key)
              } else if reflect.TypeOf(old.VA("interfaces", ifName, key)) != reflect.TypeOf(dev.VA("interfaces", ifName, key)) {
                logger.Event("if_key_type_change", ifName, "key", key,
                             "old_type", reflect.TypeOf(old.VA("interfaces", ifName, key)).String(),
                             "new_type", reflect.TypeOf(dev.VA("interfaces", ifName, key)).String())
                logger.Event("if_key_change", ifName, "key", key, "old_value", old.Vs("interfaces", ifName, key),
                                                                          "new_value", dev.Vs("interfaces", ifName, key))
              } else if old.VA("interfaces", ifName, key) != dev.VA("interfaces", ifName, key) {
                logger.Event("if_key_change", ifName, "key", key, "old_value", old.Vs("interfaces", ifName, key),
                                                                          "new_value", dev.Vs("interfaces", ifName, key))

                if key == "ifOperStatus" && old.Vi("interfaces", ifName, "ifAdminStatus") == dev.Vi("interfaces", ifName, "ifAdminStatus") {
                  alerter.Alert(dev, old.VA("interfaces", ifName, key), ifName, key)
                }
              }
            }
          }

          // check ips change
          if dev.EvM("interfaces", ifName, "ips") {
            for if_ip, _ := range dev.VM("interfaces", ifName, "ips") {
              if !old.EvM("interfaces", ifName, "ips", if_ip) {
                logger.Event("if_ip_new", ifName, "ip", if_ip, "mask", dev.Vs("interfaces", ifName, "ips", if_ip, "mask"))
              } else if dev.Vs("interfaces", ifName, "ips", if_ip, "mask") != old.Vs("interfaces", ifName, "ips", if_ip, "mask") {
                logger.Event("if_ip_mask_change", ifName, "ip", if_ip, "old_mask", old.Vs("interfaces", ifName, "ips", if_ip, "mask"),
                                                                               "new_mask", dev.Vs("interfaces", ifName, "ips", if_ip, "mask"))
              }
            }
          }
          if old.EvM("interfaces", ifName, "ips") {
            for if_ip, _ := range old.VM("interfaces", ifName, "ips") {
              if !dev.EvM("interfaces", ifName, "ips", if_ip) {
                logger.Event("if_ip_gone", ifName, "ip", if_ip, "mask", old.Vs("interfaces", ifName, "ips", if_ip, "mask"))
              }
            }
          }

          //check STP states

          var new_stp_blocked_inst = ""
          var old_stp_blocked_inst = ""

          if dev.EvA("interfaces", ifName, "stpBlockInstances") {
            inst_i64 := dev.VA("interfaces", ifName, "stpBlockInstances").([]int64)
            sort.Sort(ByInt64(inst_i64))
            s := make([]string, 0)
            for i := 0; i < len(inst_i64); i++ {
              s = append(s, strconv.FormatInt(inst_i64[i], 10))
            }
            new_stp_blocked_inst = strings.Join(s, ",")
          }

          if old.EvA("interfaces", ifName, "stpBlockInstances") {
            inst_i64 := old.VA("interfaces", ifName, "stpBlockInstances").([]int64)
            sort.Sort(ByInt64(inst_i64))
            s := make([]string, 0)
            for i := 0; i < len(inst_i64); i++ {
              s = append(s, strconv.FormatInt(inst_i64[i], 10))
            }
            old_stp_blocked_inst = strings.Join(s, ",")
          }

          if new_stp_blocked_inst != old_stp_blocked_inst {
            logger.Event("if_stp_block_change", ifName, "old_blocked_inst", old_stp_blocked_inst, "new_blocked_inst", new_stp_blocked_inst)
          }


        }
      } //dev.interfaces

      for ifName, _ := range old.VM("interfaces") {
        if !dev.EvM("interfaces", ifName) {
          logger.Event("if_gone", ifName)
        }
      }

      if dev.EvM("lldp_ports") {
        for port_index, _ := range dev.VM("lldp_ports") {
          if !old.EvM("lldp_ports", port_index) {
            //would spam from mikrotiks, say nothing
          } else {
            new_pn := make(M)
            old_pn := make(M)

            if dev.EvM("lldp_ports", port_index, "neighbours") {
              for nei, _ := range dev.VM("lldp_ports", port_index, "neighbours") {
                var key = ""
                for _, subkey := range lldpKeySet {
                  if dev.EvA("lldp_ports", port_index, "neighbours", nei, subkey) {
                    key += ":"+dev.Vs("lldp_ports", port_index, "neighbours", nei, subkey)
                  }
                }
                new_pn[key] = nei
              }
            }

            if old.EvM("lldp_ports", port_index, "neighbours") {
              for nei, _ := range old.VM("lldp_ports", port_index, "neighbours") {
                var key = ""
                for _, subkey := range lldpKeySet {
                  if old.EvA("lldp_ports", port_index, "neighbours", nei, subkey) {
                    key += ":"+old.Vs("lldp_ports", port_index, "neighbours", nei, subkey)
                  }
                }
                old_pn[key] = nei
              }
            }

            for key, nei_i := range new_pn {
              nei := nei_i.(string)
              if !old_pn.EvA(key) {
                attrs := make([]string, 0)
                attrs = append(attrs, "lldp_port_nei_new", port_index)
                for _, subkey := range lldpKeySet {
                  if dev.EvA("lldp_ports", port_index, "neighbours", nei, subkey) {
                    attrs = append(attrs, subkey, dev.Vs("lldp_ports", port_index, "neighbours", nei, subkey))
                  }
                }
                logger.Event(attrs...)
              }
            }
            for key, nei_i := range old_pn {
              nei := nei_i.(string)
              if !new_pn.EvA(key) {
                attrs := make([]string, 0)
                attrs = append(attrs, "lldp_port_nei_gone", port_index)
                for _, subkey := range lldpKeySet {
                  if old.EvA("lldp_ports", port_index, "neighbours", nei, subkey) {
                    attrs = append(attrs, subkey, old.Vs("lldp_ports", port_index, "neighbours", nei, subkey))
                  }
                }
                logger.Event(attrs...)
              }
            }
          }
        }
      }

      dev["_status_alerted_value"] = status_alerted_value
      dev["_status_alerted_time"] = status_alerted_time

      devs[dev_id] = dev
      logger.Save()
      alerter.Save()

      if reevaluate && red != nil && red.Err() == nil {

        if opt_v > 1 {
          fmt.Println("Device graph items recalc", ip, dev_id)
          fmt.Println("\t", strings.Join(graphChanges, ","))
        }
        red_args := redis.Args{}.Add("ip_graphs."+ip)
        red_args = red_args.Add("time", time.Now().Unix())

        if dev.EvM("CPUs") {
          for cpu_id, _ := range dev.VM("CPUs") {
            if gk, ok := dev.Vse("CPUs", cpu_id, "_graph_key"); ok {
              gf := esc_dev_id+"/CPU."+gk+".rrd"
              red_args = red_args.Add(gk, gf)
              dev.VM("CPUs", cpu_id)["_graph_file"] = gf
            }
          }
        }

        if dev.EvA("memoryUsed") {
          gf := esc_dev_id+"/memoryUsed.rrd"
          red_args = red_args.Add("memoryUsed.0", gf)
          dev["memoryUsed_graph_file"] = gf
        }

        if dev.EvM("interfaces") {
          for ifName, int_m := range dev.VM("interfaces") {
            int_h := int_m.(M)
            ifIndex := int_h.Vs("ifIndex")
            esc_if_name := SafeIntId(ifName)
            if ok, _ := MatchGraphIntRules(graph_int_rules, dev, ifName); ok && safeInt_regex.MatchString(esc_if_name) {
              gf_prefix := esc_dev_id+"/"+esc_if_name
              int_h["_graph_prefix"] = gf_prefix
              for _, key := range intGraphKeys {
                if int_h.EvA(key) {
                  gf := gf_prefix+"."+key+".rrd"
                  red_args = red_args.Add(key+"."+ifIndex, gf)
                }
              }
            }
          }
        }

        red.Send("MULTI")
        red.Send("DEL", "ip_graphs."+ip)
        red.Send("HSET", red_args...)
        red.Do("EXEC")

        dev["_graph_int_rules_time"] = graph_int_rules_time
      }
    }

  }

  if ip_neighbours > 0 && ip_neighbours_rule != "" {
    for ifName, int_m := range dev.VM("interfaces") {
      int_h := int_m.(M)
      if nei_s, ok := int_h.VAe("ip_neighbours"); ok {
        m := make(map[string]string)
        for _, field := range ip_neighbours_fields {
          if _, found := m[field]; !found && dev.EvA(field) {
            m[field] = AlertRuleFieldValue(dev.VA(field))
          } else if _, found := m[field]; !found && dev.EvA("interfaces", ifName, field) {
            m[field] = AlertRuleFieldValue(dev.VA("interfaces", ifName, field))
          }
        }
        for _, nei_ip := range nei_s.([]string) {
          m["neighbour_ip"] = nei_ip
          _,ignore := ip_neighbours_ignored[nei_ip]
          if !ignore && !data.EvM("dev_list", nei_ip) {
            match, err := MatchAlertRule(ip_neighbours_rule, m)
            if err == nil {
              if match {
                if opt_v > 1 {
                  fmt.Println("IP neighbour:", ifName, nei_ip, "ADD")
                }
                if opt_n {
                  red.Do("HSET", "dev_list", nei_ip, now_unix_str+":run")
                }
              } else {
                if opt_v > 2 {
                  fmt.Println("IP neighbour:", ifName, nei_ip, "NO MATCH")
                }
              }
            } else {
              if opt_v > 0 {
                fmt.Println(err)
              }
            }
          } else {
            if opt_v > 2 {
              fmt.Println("IP neighbour:", ifName, nei_ip, "SKIP")
            }
          }
        }
      }
    }
  }

  proc_time := time.Now().Sub(process_start)

  if reg_ip_dev_id && red != nil && red.Err() == nil {
    red.Do("SET", "dev_ip."+dev_id, ip)
    red.Do("SET", "ip_dev_id."+ip, dev_id)
  }

  red.Do("DEL", "ip_proc_error."+ip)

  data.VM("dev_list", ip)["proc_result"] = "done in "+strconv.FormatInt(int64(proc_time/time.Millisecond), 10)+" ms"
  data.VM("dev_list", ip)["time"] = time.Now().Unix()

  red.Do("SET", "dev_last_seen."+dev_id, strconv.FormatInt(last_seen, 10)+":"+ip)
}
