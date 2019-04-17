package communications

import (
	"bytes"
	"net/http"
	"time"

	"github.com/open-horizon/edge-sync-service/common"
	"github.com/open-horizon/edge-sync-service/core/storage"
	"github.com/open-horizon/edge-utilities/logger"
	"github.com/open-horizon/edge-utilities/logger/log"
)

// Communicator defines the interface for communications between the CSS and the CSS
type Communicator interface {
	// StartCommunication starts communications
	StartCommunication() common.SyncServiceError

	// StopCommunication stops communications
	StopCommunication() common.SyncServiceError

	// SendNotificationMessage sends a notification message from the CSS to the ESS or from the ESS to the CSS
	SendNotificationMessage(notificationTopic string, destType string, destID string, instanceID int64, metaData *common.MetaData) common.SyncServiceError

	// SendFeedbackMessage sends a feedback message from the ESS to the CSS or from the CSS to the ESS
	SendFeedbackMessage(code int, retryInterval int32, reason string, metaData *common.MetaData, sendToOrigin bool) common.SyncServiceError

	// SendErrorMessage sends an error message from the ESS to the CSS or from the CSS to the ESS
	SendErrorMessage(err common.SyncServiceError, metaData *common.MetaData, sendToOrigin bool) common.SyncServiceError

	// Register sends a registration message to be sent by an ESS
	Register() common.SyncServiceError

	// RegisterAck sends a registration acknowledgement message from the CSS
	RegisterAck(destination common.Destination) common.SyncServiceError

	// HandleRegAck handles a registration acknowledgement message from the CSS
	HandleRegAck()

	// RegisterAsNew send a notification from a CSS to a ESS that the ESS has to send a registerNew message in order
	// to register
	RegisterAsNew(destination common.Destination) common.SyncServiceError

	// RegisterNew sends a new registration message to be sent by an ESS
	RegisterNew() common.SyncServiceError

	// SendPing sends a ping message from ESS to CSS
	SendPing() common.SyncServiceError

	// GetData requests data to be sent from the CSS to the ESS or from the ESS to the CSS
	GetData(metaData common.MetaData, offset int64) common.SyncServiceError

	// SendData sends data from the CSS to the ESS or from the ESS to the CSS
	SendData(orgID string, destType string, destID string, message []byte, chunked bool) common.SyncServiceError

	// ResendObjects requests to resend all the relevant objects
	ResendObjects() common.SyncServiceError

	// SendAckResendObjects sends ack to resend objects request
	SendAckResendObjects(destination common.Destination) common.SyncServiceError

	// UpdateOrganization adds or updates an organization
	UpdateOrganization(org common.Organization, timestamp time.Time) common.SyncServiceError

	// DeleteOrganization removes an organization
	DeleteOrganization(orgID string) common.SyncServiceError
}

// Error is the error struct used by the communications code
type Error struct {
	message string
}

func (e *Error) Error() string {
	return e.message
}

// ignoredByHandler error is returned if a notification is ignored by the notification handler
type ignoredByHandler struct {
	message string
}

func (e *ignoredByHandler) Error() string {
	if e.message == "" {
		return "Ignored by notification handler"
	}
	return e.message
}

// isIgnoredByHandler returns true if the error is ignoredByHandler error
func isIgnoredByHandler(err error) bool {
	_, ok := err.(*ignoredByHandler)
	return ok
}

// Store is a reference to the Storage being used
var Store storage.Storage

// Comm is the selected communications struct
var Comm Communicator

// SendErrorResponse common code to send HTTP error codes
func SendErrorResponse(writer http.ResponseWriter, err error, message string, statusCode int) {
	if statusCode == 0 {
		switch err.(type) {
		case *common.InvalidRequest:
			statusCode = http.StatusBadRequest
		case *storage.Error:
			statusCode = http.StatusInternalServerError
		case *storage.NotConnected:
			statusCode = http.StatusServiceUnavailable
		case *ignoredByHandler:
			statusCode = http.StatusConflict
		case *Error:
			// Don't return an error if it's a communication error
			statusCode = http.StatusNoContent
			message = ""
			err = nil
		default:
			statusCode = http.StatusInternalServerError
		}
	}
	writer.WriteHeader(statusCode)

	if message != "" || err != nil {
		writer.Header().Add("Content-Type", "Text/Plain")
		buffer := bytes.NewBufferString(message)
		if err != nil {
			buffer.WriteString(err.Error())
		}
		buffer.WriteString("\n")
		writer.Write(buffer.Bytes())
	}
}

func destinationExists(orgID string, destType string, destID string) bool {
	exists, err := Store.DestinationExists(orgID, destType, destID)
	if err != nil {
		if log.IsLogging(logger.ERROR) {
			log.Error(err.Error())
		}
		return false
	} else if !exists {
		if log.IsLogging(logger.ERROR) {
			log.Error("Received message from an unknown sender: %s:%s:%s, ignoring", orgID, destType, destID)
		}
		return false
	}
	return true
}
