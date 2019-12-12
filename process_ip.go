package main

import (
  "fmt"
  "sync"
  "time"
  "regexp"
  "errors"
  "strings"
  "strconv"

  "github.com/gomodule/redigo/redis"

  w "github.com/jimlawless/whereami"
  // "github.com/davecgh/go-spew/spew"
  "github.com/fatih/color"

  . "github.com/ShyLionTjmn/aux"
  . "github.com/ShyLionTjmn/decode_dev"

)

func init() {
  w.WhereAmI()
  regexp.MustCompile("")
}

var legNeiErrNoDev = errors.New("nd")
var legNeiErrNoIfName = errors.New("nin")
var legNeiErrNoPi = errors.New("npi")

type Logger struct {
  Conn		redis.Conn
}

func (l *Logger) Event(f ... string) {
}

func (l *Logger) Save() {
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

func process_ip_data(wg *sync.WaitGroup, ip string, startup bool) {
  if wg != nil {
    defer wg.Done()
  }
  var err error
  var raw M

  var red redis.Conn

  red, err = RedisCheck(red, "unix", REDIS_SOCKET, red_db)

  if red == nil {
    if opt_v > 1 { color.Red(err.Error()) }
    return
  }

  defer func() { if red != nil { red.Close() } }()

  defer func() {
    if err != nil && red != nil && red.Err() == nil {
      ip_err := fmt.Sprintf("%d:%s ! %s", time.Now().Unix(), time.Now().Format("2006 Jan 2 15:04:05"), err.Error())
      red.Do("SET", "ip_proc_error."+ip, ip_err)

      globalMutex.Lock()
      if data.EvM("dev_list", ip) {
        data.VM("dev_list", ip)["proc_error"] = err.Error()
        data.VM("dev_list", ip)["proc_result"] = "error"
        data.VM("dev_list", ip)["time"] = time.Now().Unix()
      }
      globalMutex.Unlock()
      if opt_v > 1 { color.Red(err.Error()) }
    }
  }()

  var dev_list_state string
  dev_list_state, err = redis.String(red.Do("HGET", "dev_list", ip))
  if err != nil {
    if err == redis.ErrNil && !startup {
      //device removed from dev_list, just cleanup and return
      err = nil

      globalMutex.Lock()
      defer globalMutex.Unlock()

      //remove from dev_list
      delete(data.VM("dev_list"), ip)

      //find dev id by data_ip

      log := &Logger{Conn: red}

      for dev_id, dev_m := range devs {
        if dev_m.(M).Vs("data_ip") == ip {
          log.Event(dev_id, "dev_purged", ip)
          delete(devs, dev_id)
        }
      }

      log.Save()
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
  dl_h["state"] = dev_list_state
  dl_h["time"] = time.Now().Unix()
  globalMutex.Unlock()

  raw, err = GetRawRed(red, ip)
  if err != nil { return }

  device := Dev{ Opt_m: false, Opt_a: false, Dev_ip: ip }

  process_start := time.Now()

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

  if opt_v > 1 {
    fmt.Println("Process:", ip, "Startup:", startup)
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

  if devs.EvM(dev_id) && devs.Vs(dev_id, "data_ip") != ip {
    if opt_v > 0 {
      color.Red("CONFLICT:", devs.Vs(dev_id, "data_ip"), ip)
    }
    conflict_ip := devs.Vs(dev_id, "data_ip")
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
      data.VM("dev_list", ip)["state"] = "conflict"
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
        data.VM("dev_list", ip)["state"] = "conflict"
        return
      }
    }
  }

  // process links
  check_matrix := make(M)

  if dev.EvM("lldp_ports") && dev.Evs("locChassisId") {
    for port_index, port_h := range dev.VM("lldp_ports") {
      if port_h.(M).EvM("neighbours") {
        for _, nei_h := range port_h.(M).VM("neighbours") {
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

              //reset its status
              matrix_h["status"] = int64(2)
              matrix_h["_complete"] = int64(0)

              // we did not create it
              leg1_h := matrix_h.VM("1")
              leg1_h["PortIndex"] = port_index
              leg1_h["DevId"] = dev_id

              matrix_h["link_id"]=l2l_key(matrix_h.Vs("0", "DevId"), matrix_h.Vs("0", "PortIndex"), matrix_h.Vs("1", "DevId"), matrix_h.Vs("1", "PortIndex"))

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
                    if opt_v > 1 {
                      color.Green("link established by meeting")
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
          }

          matrix_h["_creator"] = dev_id
          matrix_h["_complete"] = int64(0)
          matrix_h["_alt"] = int64(0)
          matrix_h["status"] = int64(2)
          matrix_h["matrix_id"] = norm_matrix_id
          matrix_h["_alt_matrix_id"] = alt_matrix_id
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
            leg1_h["PortIndex"] = nei_port_index
            matrix_h["link_id"]=l2l_key(matrix_h.Vs("0", "DevId"), matrix_h.Vs("0", "PortIndex"), matrix_h.Vs("1", "DevId"), matrix_h.Vs("1", "PortIndex"))
            if nei_error != legNeiErrNoIfName {
              leg1_h["ifName"] = nei_ifname
              if port_h.(M).Evs("ifName") {
                matrix_h["status"] = int64(1)
                matrix_h["_complete"] = int64(1)
                check_matrix[norm_matrix_id] = dev_id
                if opt_v > 1 {
                  if pass == 0 {
                    color.Green("link established by creator")
                  } else {
                    color.Green("link updated by creator")
                  }
                }
              }
            }
          }

          if norm_matrix_id != alt_matrix_id && matrix_h["_complete"] != 1 {
            alt_matrix_h := matrix_h.Copy()
            alt_matrix_h["_alt"] = int64(1)
            alt_matrix_h["_alt_matrix_id"] = norm_matrix_id
            alt_matrix_h["matrix_id"] = alt_matrix_id
            l2Matrix[alt_matrix_id] = alt_matrix_h
          }
        }
      }
    }
  }

  //copy links from previous run and cleanup outdated
  //if !startup && devs.EvM(dev_id, "interfaces") {
  if !startup && devs.EvM(dev_id) {
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

              matrix_id := link_h.Vs("matrix_id")

              delete(data.VM("l2_links"), link_id)

              delete(l2Matrix, matrix_id)

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
    matrix_h := l2Matrix.VM(matrix_id)
    link_id := matrix_h.Vs("link_id")

    if check_matrix.Vs(matrix_id) == dev_id {
      if0_h = dev.VM("interfaces", matrix_h.Vs("0", "ifName"))
      if1_h = devs.VM(matrix_h.Vs("1", "DevId"), "interfaces", matrix_h.Vs("1", "ifName"))
    } else {
      if0_h = devs.VM(matrix_h.Vs("0", "DevId"), "interfaces", matrix_h.Vs("0", "ifName"))
      if1_h = dev.VM("interfaces", matrix_h.Vs("1", "ifName"))
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

  if !devs.EvM(dev_id) {
    devs[dev_id] = dev
  } else {
    // check what's changed
    devs[dev_id] = dev
  }

  proc_time := time.Now().Sub(process_start)

  data.VM("dev_list", ip)["proc_result"] = "done in "+strconv.FormatInt(int64(proc_time/time.Millisecond), 10)+" ms"
  data.VM("dev_list", ip)["time"] = time.Now().Unix()
}
