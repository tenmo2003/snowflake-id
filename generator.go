package snowflakeid

import (
	"strconv"
	"sync"
	"time"
)

const SIGN_BIT = 1
const TIMESTAMP_BIT = 41
const MACHINE_ID_BIT = 10
const SEQUENCE_BIT = 12

const MAX_MACHINE_ID = int64(1<<MACHINE_ID_BIT) - 1
const MAX_SEQUENCE = int64(1<<SEQUENCE_BIT) - 1

type SnowflakeIDGenerator struct {
	chosenEpoch            *time.Time
	machineID              int64
	currentSequenceNumber  int64
	lastGeneratedTimestamp int64
	mut                    sync.Mutex
}

func NewGenerator(chosenEpoch *time.Time, machineID int64) *SnowflakeIDGenerator {
	if machineID > MAX_MACHINE_ID {
		panic("machineID must be less than " + strconv.FormatInt(MAX_MACHINE_ID, 10))
	}

	return &SnowflakeIDGenerator{
		chosenEpoch: chosenEpoch,
		machineID:   machineID,
	}
}

func (g *SnowflakeIDGenerator) GenerateID() int64 {
	seq, timestamp := g.getNextSequenceNumber()

	machineIDMask := int64(1<<(MACHINE_ID_BIT+SEQUENCE_BIT) - 1)
	paddedMachineID := (machineIDMask & g.machineID) << SEQUENCE_BIT

	timestampMask := int64(1<<(TIMESTAMP_BIT+MACHINE_ID_BIT+SEQUENCE_BIT) - 1)
	paddedTimestamp := (timestampMask & timestamp) << (MACHINE_ID_BIT + SEQUENCE_BIT)

	return paddedMachineID | paddedTimestamp | seq
}

func (g *SnowflakeIDGenerator) getNextSequenceNumber() (int64, int64) {
	g.mut.Lock()
	defer g.mut.Unlock()

	currentTimestamp := g.timestamp()

	if currentTimestamp < g.lastGeneratedTimestamp {
		panic("Invalid System Clock!")
	}

	if currentTimestamp > g.lastGeneratedTimestamp {
		g.currentSequenceNumber = 0
	} else {
		g.currentSequenceNumber = (g.currentSequenceNumber + 1) & MAX_SEQUENCE
		if g.currentSequenceNumber == 0 {
			currentTimestamp = g.waitUntilNextMillisecond(currentTimestamp)
		}
	}

	g.lastGeneratedTimestamp = currentTimestamp

	return g.currentSequenceNumber, currentTimestamp
}

func (g *SnowflakeIDGenerator) waitUntilNextMillisecond(timeFromEpoch int64) int64 {
	for {
		timeFromEpoch = g.timestamp()
		if timeFromEpoch != g.lastGeneratedTimestamp {
			return timeFromEpoch
		}
	}
}

func (g *SnowflakeIDGenerator) timestamp() int64 {
	return time.Since(*g.chosenEpoch).Milliseconds()
}
