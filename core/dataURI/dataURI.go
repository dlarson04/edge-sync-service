package dataURI

import (
	"fmt"
	"io"
	"net/url"
	"os"
	"time"
	"strings"
	"io/ioutil"

	"github.com/open-horizon/edge-sync-service/common"
	"github.com/open-horizon/edge-utilities/logger"
	"github.com/open-horizon/edge-utilities/logger/trace"
)

// Error is the error used in the data URI package
type Error struct {
	message string
}

func (e *Error) Error() string {
	return e.message
}

// AppendData appends a chunk of data to the file stored at the given URI
func AppendData(uri string, dataReader io.Reader, dataLength uint32, offset int64, total int64, isFirstChunk bool, isLastChunk bool) common.SyncServiceError {
	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Storing data chunk at %s", uri)
	}
	trace.Info("Storing data chunk at %s", uri)

	dataURI, err := url.Parse(uri)
	if err != nil || !strings.EqualFold(dataURI.Scheme, "file") {
		return &Error{"Invalid data URI"}
	}

	filePath := dataURI.Path + ".tmp"
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return common.CreateError(err, fmt.Sprintf("Failed to open file %s to append data. Error: ", dataURI.Path))
	}
	defer file.Close()
	if _, err = file.Seek(offset, io.SeekStart); err != nil {
		return &common.IOError{Message: fmt.Sprintf("Failed to seek to the offset %d of a file. Error: %s", offset, err.Error())}
	}

	written, err := io.Copy(file, dataReader)
	trace.Info("Append data - written - %d, dataLength - %d", written, dataLength)
	if err != nil && err != io.EOF {
		return &common.IOError{Message: "Failed to write to file. Error: " + err.Error()}
	}
	if written != int64(dataLength) {
		return &common.IOError{Message: "Failed to write all the data to file."}
	}

	if isLastChunk {
		if err := os.Rename(filePath, dataURI.Path); err != nil {
			return &common.IOError{Message: "Failed to rename data file. Error: " + err.Error()}
		}
	}
	return nil
}

// StoreData writes the data to the file stored at the given URI
func StoreData(uri string, dataReader io.Reader, dataLength uint32) (int64, common.SyncServiceError) {
	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Storing data at %s", uri)
	}
	trace.Info("Storing data at %s", uri)
	dataURI, err := url.Parse(uri)
	if err != nil || !strings.EqualFold(dataURI.Scheme, "file") {
		return 0, &Error{"Invalid data URI"}
	}

	filePath := dataURI.Path + ".tmp"
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return 0, common.CreateError(err, fmt.Sprintf("Failed to open file %s to write data. Error: ", dataURI.Path))
	}
	defer file.Close()

	fi,err := file.Stat()
	if err != nil {
		return 0, common.CreateError(err, fmt.Sprintf("Failed to get file Stat on file %s. Error: ", dataURI.Path))
	}

	var currentWritten int64 = 0
	if fi != nil {
		currentWritten = fi.Size()
	}
	trace.Info("Current file size is %d bytes", currentWritten)

	start_time := time.Now().Unix()
	var hasRead int64 = 0
	if currentWritten == 0 {
		if _, err = file.Seek(0, io.SeekStart); err != nil {
			return 0, &common.IOError{Message: "Failed to seek to the start of a file. Error: " + err.Error()}
		}
	} else {

		//var b bytes.Buffer
		//theWriter := bufio.NewWriter(&b)

		//var readLimit int64 = 1024 * 1024 * 32
		//if currentWritten <  readLimit {
		//	readLimit = currentWritten
		//}

		doOnce  := true

		for hasRead < currentWritten {
			if written, err := io.CopyN(ioutil.Discard, dataReader, currentWritten); err != nil {
				return 0, &common.IOError{Message: "Failed to write to file. Error: " + err.Error()}
			} else {
				hasRead += written
				if doOnce {
					trace.Info("Gotten read bytes from CopyN of %d", written)
					doOnce = false
				}
			}

			//b.Reset()

			//if (hasRead + readLimit) > currentWritten {
			//	readLimit = currentWritten - hasRead
			//}
		}
	}

	stop_time := time.Now().Unix() 
	trace.Info("Store data - Read resume total of %d bytes in %d seconds", hasRead, stop_time - start_time)

	trace.Info("Now getting into writing to the file after siphoning off %d bytes", hasRead)
	written, err := io.Copy(file, dataReader)
	written += hasRead
	trace.Info("Store data - written - %d, dataLength - %d", written, dataLength)
	if err != nil && err != io.EOF {
	        trace.Info("error - %v", err)
		return 0, &common.IOError{Message: "Failed to write to file. Error: " + err.Error()}
	}
	if written != int64(dataLength) && dataLength != 0 {
		return 0, &common.IOError{Message: "Failed to write all the data to file."}
	}
	trace.Info("Store data ... Must be done with write")
	if err := os.Rename(filePath, dataURI.Path); err != nil {
		return 0, &common.IOError{Message: "Failed to rename data file. Error: " + err.Error()}
	}
	return written, nil
}

// StoreTempData writes the data to the tmp file stored at the given URI
func StoreTempData(uri string, dataReader io.Reader, dataLength uint32) (int64, common.SyncServiceError) {
	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Storing data at %s", uri)
	}
	trace.Info("Store Temp data at %s", uri)
	dataURI, err := url.Parse(uri)
	if err != nil || !strings.EqualFold(dataURI.Scheme, "file") {
		return 0, &Error{"Invalid data URI"}
	}

	filePath := dataURI.Path + ".tmp"
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return 0, common.CreateError(err, fmt.Sprintf("Failed to open file %s to write data. Error: ", dataURI.Path))
	}
	defer file.Close()

	if _, err = file.Seek(0, io.SeekStart); err != nil {
		return 0, &common.IOError{Message: "Failed to seek to the start of a file. Error: " + err.Error()}
	}

	written, err := io.Copy(file, dataReader)
	trace.Info("Store temp data - written - %d, dataLength - %d", written, dataLength)
	if err != nil && err != io.EOF {
	        trace.Info("error - %v", err)
		return 0, &common.IOError{Message: "Failed to write to file. Error: " + err.Error()}
	}
	if written != int64(dataLength) && dataLength != 0 {
		return 0, &common.IOError{Message: "Failed to write all the data to file."}
	}
	trace.Info("Store temp data ... Must be done with write")
	return written, nil
}

// StoreDataFromTempData rename {dataURI.Path}.tmp to {dataURI.Path}
func StoreDataFromTempData(uri string) common.SyncServiceError {
	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Storing data from temp data at %s", uri)
	}
	dataURI, err := url.Parse(uri)
	if err != nil || !strings.EqualFold(dataURI.Scheme, "file") {
		return &Error{"Invalid data URI"}
	}

	tmpFilePath := dataURI.Path + ".tmp"

	if err := os.Rename(tmpFilePath, dataURI.Path); err != nil {
		return &common.IOError{Message: "Failed to rename data file. Error: " + err.Error()}
	}

	return nil
}

// GetData retrieves the data stored at the given URI.
// After reading, the reader has to be closed.
func GetData(uri string) (io.Reader, common.SyncServiceError) {
	dataURI, err := url.Parse(uri)
	if err != nil || !strings.EqualFold(dataURI.Scheme, "file") {
		return nil, &Error{"Invalid data URI"}
	}

	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Retrieving data from %s", uri)
	}

	file, err := os.Open(dataURI.Path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, &common.NotFound{}
		}
		return nil, common.CreateError(err, fmt.Sprintf("Failed to open file %s to read data. Error: ", dataURI.Path))
	}
	return file, nil
}

// GetDataChunk retrieves the data stored at the given URI.
// After reading, the reader has to be closed.
func GetDataChunk(uri string, size int, offset int64) ([]byte, bool, int, common.SyncServiceError) {
	dataURI, err := url.Parse(uri)
	if err != nil || !strings.EqualFold(dataURI.Scheme, "file") {
		return nil, false, 0, &Error{"Invalid data URI"}
	}

	if trace.IsLogging(logger.TRACE) {
		trace.Trace("Retrieving data from %s", uri)
	}

	file, err := os.Open(dataURI.Path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, true, 0, &common.NotFound{}
		}
		return nil, true, 0, common.CreateError(err, fmt.Sprintf("Failed to open file %s to read data. Error: ", dataURI.Path))
	}
	defer file.Close()

	eof := false
	result := make([]byte, size)
	n, err := file.ReadAt(result, offset)
	if n == size {
		if err != nil { // This, most probably, can never happen when n == size, but the doc doesn't say it
			return nil, true, 0, &common.IOError{Message: "Failed to read data. Error: " + err.Error()}
		}
		var fi os.FileInfo
		fi, err = file.Stat()
		if err == nil && fi.Size() == offset+int64(size) {
			eof = true
		}
	} else {
		// err != nil is always true when n<size
		if err == io.EOF {
			eof = true
		} else {
			return nil, true, 0, &common.IOError{Message: "Failed to read data. Error: " + err.Error()}
		}
	}

	return result, eof, n, nil
}

// DeleteStoredData deletes the data file stored at the given URI
func DeleteStoredData(uri string) common.SyncServiceError {
	dataURI, err := url.Parse(uri)
	if err != nil || !strings.EqualFold(dataURI.Scheme, "file") {
		return &Error{"Invalid data URI"}
	}
	if err = os.Remove(dataURI.Path); err != nil && !os.IsNotExist(err) {
		return &common.IOError{Message: "Failed to delete data. Error: " + err.Error()}
	}
	return nil
}
