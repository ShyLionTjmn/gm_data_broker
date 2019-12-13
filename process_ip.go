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

var scalarWatchKeys = []string{"sysName",", ""locChassisSysName", "snmpEngineId", "sysLocation", "locChassisIdSubtype", "sysDescr",
                               "locChassisId", "overall_status", "sysObjectID", "sysContact", "CiscoConfSave", "data_ip", "short_name",
                               "powerState",
                              }

const NL="\n"

type Logger struct {
  Conn		redis.Conn
}

func (l *Logger) Event(f ... string) { // dev_id, "event", key|"", "attr", value, "attr", value, ...
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
          log.Event(dev_id, "dev_purged", "", "ip", ip)
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

    return
  }

  device := Dev{ Opt_m: true, Opt_a: true, Dev_ip: ip }

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
    fmt.Println("Process:", ip)
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
      color.Red("CONFLICT: %s vs %s", devs.Vs(dev_id, "data_ip"), ip)
    }
    conflict_ip := devs.Vs(dev_id, "data_ip")
    //there is duplicate device id
    if overall_status == "ok" && devs.Vs(dev_id, "overall_status") != "ok" {
      // duplicate device is old, overwrite it
      wipe_dev(dev_id)
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
        wipe_dev(dev_id)
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

  //check for id change
  if prev_id, ok := data.Vse("dev_list", ip , "id"); ok && prev_id != dev_id {
    wipe_dev(prev_id)
    if opt_v > 0 {
      color.Yellow("Dev id changed. Previous data purged: %s, %s", prev_id, ip)
    }
  } else {
    data.VM("dev_list", ip)["id"] = dev_id
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

              prev_complete := matrix_h.Vi("_complete")
              //reset its status
              matrix_h["status"] = int64(2)
              matrix_h["_complete"] = int64(0)

              // we did not create it
              leg1_h := matrix_h.VM("1")
              leg1_h["PortIndex"] = port_index
              leg1_h["DevId"] = dev_id

              dev_refs.MkM(dev_id, "l2Matrix", matrix_id)

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
            matrix_h["link_id"]=l2l_key(matrix_h.Vs("0", "DevId"), matrix_h.Vs("0", "PortIndex"), matrix_h.Vs("1", "DevId"), matrix_h.Vs("1", "PortIndex"))
            if nei_error != legNeiErrNoIfName {
              leg1_h["ifName"] = nei_ifname
              if port_h.(M).Evs("ifName") {
                matrix_h["status"] = int64(1)
                matrix_h["_complete"] = int64(1)
                check_matrix[norm_matrix_id] = dev_id
                if opt_v > 1 {
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
  //if !startup && devs.EvM(dev_id, "interfaces") {
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
              if opt_v > 1 {
                color.Magenta("link down: %s", link_id)
              }
            }
          } else {
            if opt_v > 1 {
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

  now_unix := time.Now().Unix()

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

  if startup {
    devs[dev_id] = dev
  } else {
    logger := &Logger{Conn: red}

    if !devs.EvM(dev_id) {
      devs[dev_id] = dev
      location, _ := dev.Vse("sysLocation")
      logger.Event(dev_id, "dev_new", "", "ip", ip, "short_name", dev.Vs("short_name"), "loc", location)
    } else {
      // check what's changed

      old := devs.VM(dev_id)

      for _, key := range scalarWatchKeys {
        if old.EvA(key) && !dev.EvA(key) {
          logger.Event(dev_id, "key_gone", key, "old_value", old.Vs(key))
        } else if !old.EvA(key) && dev.EvA(key) {
          logger.Event(dev_id, "key_new", key, "new_value", dev.Vs(key))
        } else if old.EvA(key) && dev.EvA(key) && reflect.TypeOf(old.VA(key)) != reflect.TypeOf(dev.VA(key)) {
          logger.Event(dev_id, "key_type_change", key, "olt_type", reflect.TypeOf(old.VA(key)).String(), "new_type", reflect.TypeOf(dev.VA(key)).String())
          logger.Event(dev_id, "key_change", key, "old_value", old.Vs(key), "new_value", dev.Vs(key))
        } else if old.EvA(key) && dev.EvA(key) && old.VA(key) != dev.VA(key) {
          logger.Event(dev_id, "key_change", key, "old_value", old.Vs(key), "new_value", dev.Vs(key))

          //alert if changes
          dev_alert(dev, old, "", key, old.Vs(key), dev.Vs(key))
        }
      }



      devs[dev_id] = dev
    }

    logger.Save()
  }

  proc_time := time.Now().Sub(process_start)

  data.VM("dev_list", ip)["proc_result"] = "done in "+strconv.FormatInt(int64(proc_time/time.Millisecond), 10)+" ms"
  data.VM("dev_list", ip)["time"] = time.Now().Unix()
}
