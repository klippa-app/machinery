package log_test

import (
	"testing"

	"github.com/RichardKnop/logging"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/stretchr/testify/assert"
)

type fakeLogger struct {
	calls []string
}

func (f *fakeLogger) Print(v ...interface{})                 { f.calls = append(f.calls, "Print") }
func (f *fakeLogger) Printf(format string, v ...interface{}) { f.calls = append(f.calls, "Printf") }
func (f *fakeLogger) Println(v ...interface{})               { f.calls = append(f.calls, "Println") }
func (f *fakeLogger) Fatal(v ...interface{})                 { f.calls = append(f.calls, "Fatal") }
func (f *fakeLogger) Fatalf(format string, v ...interface{}) { f.calls = append(f.calls, "Fatalf") }
func (f *fakeLogger) Fatalln(v ...interface{})               { f.calls = append(f.calls, "Fatalln") }
func (f *fakeLogger) Panic(v ...interface{})                 { f.calls = append(f.calls, "Panic") }
func (f *fakeLogger) Panicf(format string, v ...interface{}) { f.calls = append(f.calls, "Panicf") }
func (f *fakeLogger) Panicln(v ...interface{})               { f.calls = append(f.calls, "Panicln") }

func TestDefaultLogger(t *testing.T) {
	log.INFO.Print("should not panic")
	log.WARNING.Print("should not panic")
	log.ERROR.Print("should not panic")
	log.FATAL.Print("should not panic")
}

func TestSet(t *testing.T) {
	custom := new(fakeLogger)
	log.Set(custom)

	log.DEBUG.Print("debug")
	log.INFO.Print("info")
	log.WARNING.Print("warning")
	log.ERROR.Print("error")
	log.FATAL.Print("fatal")

	var custom2 logging.LoggerInterface = custom
	assert.Equal(t, custom2, log.DEBUG)
	assert.Equal(t, custom2, log.INFO)
	assert.Equal(t, custom2, log.WARNING)
	assert.Equal(t, custom2, log.ERROR)
	assert.Equal(t, custom2, log.FATAL)
}

func TestSetDebug(t *testing.T) {
	custom := new(fakeLogger)
	log.SetDebug(custom)

	var expected logging.LoggerInterface = custom
	assert.Equal(t, expected, log.DEBUG)
}

func TestSetInfo(t *testing.T) {
	custom := new(fakeLogger)
	log.SetInfo(custom)

	var expected logging.LoggerInterface = custom
	assert.Equal(t, expected, log.INFO)
}

func TestSetWarning(t *testing.T) {
	custom := new(fakeLogger)
	log.SetWarning(custom)

	var expected logging.LoggerInterface = custom
	assert.Equal(t, expected, log.WARNING)
}

func TestSetError(t *testing.T) {
	custom := new(fakeLogger)
	log.SetError(custom)

	var expected logging.LoggerInterface = custom
	assert.Equal(t, expected, log.ERROR)
}

func TestSetFatal(t *testing.T) {
	custom := new(fakeLogger)
	log.SetFatal(custom)

	var expected logging.LoggerInterface = custom
	assert.Equal(t, expected, log.FATAL)
}
