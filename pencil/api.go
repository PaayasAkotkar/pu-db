package pencil

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const ColorReset = "\033[0m"

const (
	Red    = "#FF0000"
	Orange = "#FF7F00"
	Yellow = "#FFFF00"
	Green  = "#00FF00"
	Blue   = "#0000FF"
	Indigo = "#4B0082"
	Violet = "#9400D3"
)

func HexToANSI(hex string) string {
	hex = strings.TrimPrefix(hex, "#")
	if len(hex) != 6 {
		return ""
	}

	r, _ := strconv.ParseInt(hex[0:2], 16, 64)
	g, _ := strconv.ParseInt(hex[2:4], 16, 64)
	b, _ := strconv.ParseInt(hex[4:6], 16, 64)

	// \033[38;2;R;G;Bm is the ANSI truecolor escape sequence
	return fmt.Sprintf("\033[38;2;%d;%d;%dm", r, g, b)
}

type mockClock struct {
	fixedTime time.Time
}

func (m mockClock) Now() time.Time {
	return m.fixedTime
}

func (m mockClock) NewTicker(duration time.Duration) *time.Ticker {
	return time.NewTicker(duration)
}
func Print(hex string, message string) {
	ansi := HexToANSI(hex)
	fmt.Printf("%s%s%s\n", ansi, message, ColorReset)
}

func Log(hex string, message string) {
	ansi := HexToANSI(hex)
	log.Printf("%s%s%s", ansi, message, ColorReset)
}

type Pencil struct {
	sug *zap.SugaredLogger
}

func New() *Pencil {
	timeLayout := "Jan Mon 2006: 03:04PM"

	formattedNow := time.Now().Format(timeLayout)
	parsedTime, err := time.Parse(timeLayout, formattedNow)
	if err != nil {
		panic(err)
	}
	clock := mockClock{fixedTime: parsedTime}

	cf := zapcore.EncoderConfig{
		TimeKey:          "ts",
		EncodeTime:       zapcore.TimeEncoderOfLayout(timeLayout),
		LevelKey:         "",
		CallerKey:        "caller",
		EncodeCaller:     zapcore.ShortCallerEncoder,
		MessageKey:       "message",
		ConsoleSeparator: " ",
		LineEnding:       zapcore.DefaultLineEnding,
	}

	core := zapcore.NewCore(
		zapcore.NewConsoleEncoder(cf),
		zapcore.Lock(os.Stdout),
		zap.DebugLevel,
	)

	logger := zap.New(
		core,
		zap.WithClock(clock),
		zap.AddCaller(),
		zap.AddCallerSkip(0),
	)
	return &Pencil{
		sug: logger.Sugar(),
	}
}

func (p *Pencil) Pen(hexCode string, args ...any) {
	ansi := HexToANSI(hexCode)
	msg := fmt.Sprint(args...)
	p.sug.Infof("%s%s%s", ansi, msg, ColorReset)
}
func (p *Pencil) Panic(hexCode string, args ...any) {
	ansi := HexToANSI(hexCode)
	msg := fmt.Sprint(args...)
	p.sug.Panicf("%s%s%s", ansi, msg, ColorReset)
}
