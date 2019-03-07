package base

import (
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/open-horizon/edge-sync-service/common"
	"github.com/open-horizon/edge-sync-service/core/communications"
	"github.com/open-horizon/edge-sync-service/core/leader"
	"github.com/open-horizon/edge-sync-service/core/security"
	"github.com/open-horizon/edge-sync-service/core/storage"
	"github.com/open-horizon/edge-utilities/logger"
	"github.com/open-horizon/edge-utilities/logger/trace"
)

var store storage.Storage
var communication communications.Communicator

var resendTimer *time.Timer
var resendStopChannel chan int

var activateTimer *time.Timer
var activateStopChannel chan int

var maintenanceTimer *time.Timer
var maintenanceStopChannel chan int

var pingTicker *time.Ticker
var pingStopChannel chan int

var removeESSTicker *time.Ticker
var removeESSStopChannel chan int

var waitingOnBlockChannel bool
var blockChannel chan int

var startStopLock sync.Mutex
var started bool

func init() {
	blockChannel = make(chan int, 1)
	resendStopChannel = make(chan int, 1)
	activateStopChannel = make(chan int, 1)
	maintenanceStopChannel = make(chan int, 1)
	pingStopChannel = make(chan int, 1)
	removeESSStopChannel = make(chan int, 1)
}

// Start starts up the synnc service
func Start(swaggerFile string, registerHandlers bool) common.SyncServiceError {
	startStopLock.Lock()
	defer startStopLock.Unlock()

	common.ResetGoRoutineCounter()

	if started {
		return &common.SetupError{Message: "An attempt was made to start the Sync Service after it was already started"}
	}

	var ipAddress string
	var err error
	if common.Configuration.ListeningType != common.ListeningUnix &&
		common.Configuration.ListeningType != common.ListeningSecureUnix {
		ipAddress, err = checkIPAddress(common.Configuration.ListeningAddress)
		if err != nil {
			return err
		}
	} else {
		ipAddress = ""
	}

	if err = setupCertificates(); err != nil {
		return &common.SetupError{Message: fmt.Sprintf("Failed to create TLS certificate. Error: %s\n", err.Error())}
	}

	security.Start()

	if common.Configuration.NodeType == common.CSS {
		mongoStore := &storage.MongoStorage{}
		if common.Configuration.CommunicationProtocol == common.HybridMQTT ||
			common.Configuration.CommunicationProtocol == common.HybridWIoTP {
			store = &storage.Cache{Store: mongoStore}
		} else {
			store = mongoStore
		}
	} else {
		if common.Configuration.ESSPersistentStorage {
			store = &storage.BoltStorage{}
		} else {
			store = &storage.InMemoryStorage{}
		}
	}

	if err := store.Init(); err != nil {
		return &common.SetupError{Message: fmt.Sprintf("Failed to initialize storage driver. Error: %s\n", err.Error())}
	}
	communications.Store = store
	security.Store = store

	leader.StartLeaderDetermination(store)

	var mqttComm *communications.MQTT
	if (common.Configuration.NodeType == common.ESS && common.Configuration.CommunicationProtocol != common.HTTPProtocol) ||
		(common.Configuration.NodeType == common.CSS && common.Configuration.CommunicationProtocol != common.HTTPProtocol) {
		mqttComm = &communications.MQTT{}
		if err := mqttComm.StartCommunication(); err != nil {
			return &common.SetupError{Message: fmt.Sprintf("Failed to initialize MQTT communication driver. Error: %s\n", err.Error())}
		}
	}

	var httpComm *communications.HTTP
	if (common.Configuration.NodeType == common.ESS && common.Configuration.CommunicationProtocol == common.HTTPProtocol) ||
		(common.Configuration.NodeType == common.CSS &&
			(common.Configuration.CommunicationProtocol == common.HTTPProtocol ||
				common.Configuration.CommunicationProtocol == common.HybridMQTT ||
				common.Configuration.CommunicationProtocol == common.HybridWIoTP)) {
		httpComm = &communications.HTTP{}
		if err := httpComm.StartCommunication(); err != nil {
			return &common.SetupError{Message: fmt.Sprintf("Failed to initialize HTTP communication driver. Error: %s\n", err.Error())}
		}
	}

	communication = communications.NewWrapper(httpComm, mqttComm)
	communications.Comm = communication

	if common.Configuration.NodeType == common.ESS {
		common.Registered = false
		if common.Configuration.CommunicationProtocol == common.HTTPProtocol {
			communication.Register()
		}
	}

	common.ResendAcked = true

	go func() {
		common.GoRoutineStarted()
		keepRunning := true
		for keepRunning {
			resendTimer = time.NewTimer(time.Second * time.Duration(common.Configuration.ResendInterval))
			select {
			case <-resendTimer.C:
				communications.ResendNotifications()

			case <-resendStopChannel:
				keepRunning = false
			}
		}
		resendTimer = nil
		common.GoRoutineEnded()
	}()

	go func() {
		common.GoRoutineStarted()
		keepRunning := true
		for keepRunning {
			activateTimer = time.NewTimer(time.Second * time.Duration(common.Configuration.ObjectActivationInterval))
			select {
			case <-activateTimer.C:
				communications.ActivateObjects()

			case <-activateStopChannel:
				keepRunning = false
			}
		}
		activateTimer = nil
		common.GoRoutineEnded()
	}()

	if common.Configuration.NodeType == common.CSS {
		go func() {
			common.GoRoutineStarted()
			keepRunning := true
			for keepRunning {
				maintenanceTimer = time.NewTimer(time.Second * time.Duration(common.Configuration.StorageMaintenanceInterval))
				select {
				case <-maintenanceTimer.C:
					store.PerformMaintenance()

				case <-maintenanceStopChannel:
					keepRunning = false
				}
			}
			maintenanceTimer = nil
			common.GoRoutineEnded()
		}()
	}

	if common.Configuration.NodeType == common.ESS {
		pingTicker = time.NewTicker(time.Hour * time.Duration(common.Configuration.ESSPingInterval))
		go func() {
			common.GoRoutineStarted()
			keepRunning := true
			for keepRunning {
				select {
				case <-pingTicker.C:
					communications.Comm.SendPing()

				case <-pingStopChannel:
					keepRunning = false
				}
			}
			pingTicker = nil
			common.GoRoutineEnded()
		}()
	}

	if common.Configuration.NodeType == common.CSS && common.Configuration.RemoveESSRegistrationTime > 0 {
		removeESSTicker = time.NewTicker(time.Hour * 24 * time.Duration(common.Configuration.RemoveESSRegistrationTime))
		lastTimestamp := time.Now()
		go func() {
			common.GoRoutineStarted()
			keepRunning := true
			for keepRunning {
				select {
				case <-removeESSTicker.C:
					if leader.CheckIfLeader() {
						store.RemoveInactiveDestinations(lastTimestamp)
					}
					lastTimestamp = time.Now()

				case <-removeESSStopChannel:
					keepRunning = false
				}
			}
			removeESSTicker = nil
			common.GoRoutineEnded()
		}()
	}

	err = startHTTPServer(ipAddress, registerHandlers, swaggerFile)
	if err == nil {
		common.Running = true
		started = true
	}
	return err
}

// Stop stops the Sync Server
func Stop(quiesceTime int) {
	startStopLock.Lock()
	defer startStopLock.Unlock()

	common.Running = false

	leader.StopLeadershipPeriodicUpdate()

	if trace.IsLogging(logger.INFO) {
		trace.Info("Stopping in %d seconds", quiesceTime)
	}

	if quiesceTime > 0 {
		timer := time.NewTimer(time.Duration(quiesceTime) * time.Second)
		<-timer.C
	}

	stopHTTPServing()

	communication.StopCommunication()

	security.Stop()

	if resendTimer != nil {
		resendTimer.Stop()
		resendStopChannel <- 1
	}
	if activateTimer != nil {
		activateTimer.Stop()
		activateStopChannel <- 1
	}
	if maintenanceTimer != nil {
		maintenanceTimer.Stop()
		maintenanceStopChannel <- 1
	}
	if pingTicker != nil {
		pingTicker.Stop()
		pingStopChannel <- 1
	}
	if removeESSTicker != nil {
		removeESSTicker.Stop()
		removeESSStopChannel <- 1
	}

	common.BlockUntilNoRunningGoRoutines()

	store.Stop()

	if waitingOnBlockChannel {
		blockChannel <- 1
	}

	started = false
}

// BlockUntilShutdown blocks the current "thread"
func BlockUntilShutdown() {
	waitingOnBlockChannel = true
	_ = <-blockChannel
}

func checkIPAddress(host string) (string, common.SyncServiceError) {

	if host == "" {
		h, err := os.Hostname()
		if err != nil {
			return "", &common.SetupError{Message: fmt.Sprintf("Failed to get hostname. Error: %s\n", err.Error())}
		}
		host = h
	}

	ips, err := net.LookupIP(host)
	if err != nil {
		return "", &common.SetupError{Message: fmt.Sprintf("Failed to get IP for the host. Error: %s\n", err.Error())}
	}

	ipAddress := ""
	for _, ip := range ips {
		if ip4 := ip.To4(); ip4 != nil {
			ipAddress = ip4.String()
		}
	}

	if ipAddress == "" {
		return "", &common.SetupError{Message: fmt.Sprintf("Failed to get IP for the host.")}
	}
	return ipAddress, nil
}
