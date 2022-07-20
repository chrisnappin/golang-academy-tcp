package server

import (
	"errors"
	"log"
	"strconv"
	"strings"
)

type command int

const (
	putCommand    command = iota
	getCommand    command = iota
	deleteCommand command = iota
	closeCommand  command = iota
)

type commandRequest struct {
	command command
	key     string
	value   string
	length  int
}

var errUnrecognisedCommand = errors.New("unrecognised command")

// parseCommand parses the string supplied, looking for a valid key store command,
// with 3 possible outcomes: a command is found, no command is found (incomplete data,
// read more input then try again), or an error (invalid command).
func parseCommand(buffer string) (*commandRequest, error) {
	var command *commandRequest

	var incomplete bool

	var err error

	switch {
	case strings.HasPrefix(buffer, "put"):
		command, incomplete, err = parsePutCommand(buffer)

	case strings.HasPrefix(buffer, "get"):
		command, incomplete, err = parseGetCommand(buffer)

	case strings.HasPrefix(buffer, "del"):
		command, incomplete, err = parseDeleteCommand(buffer)

	case strings.HasPrefix(buffer, "bye"):
		command = &commandRequest{closeCommand, "", "", 0}

	default:
		if len(buffer) > 2 {
			// 3 or more characters that didn't match above, so can't be a valid command
			log.Printf("Unrecognised command %s", buffer)

			err = errUnrecognisedCommand
		}

		// otherwise might be an incomplete command
		incomplete = true
	}

	if err != nil {
		return nil, err
	}

	if incomplete {
		return nil, nil
	}

	return command, nil
}

func parsePutCommand(buffer string) (*commandRequest, bool, error) {
	argument1, remaining, incomplete, err := parseArgument(buffer[3:])
	if err != nil {
		log.Println("Error with argument 1 of put command: ", err)
		return nil, false, err
	}

	if incomplete {
		return nil, true, nil
	}

	argument2, _, incomplete, err := parseArgument(remaining)
	if err != nil {
		log.Println("Error with argument 2 of put command: ", err)
		return nil, false, err
	}

	if incomplete {
		return nil, true, nil
	}

	return &commandRequest{putCommand, argument1, argument2, 0}, false, nil
}

func parseGetCommand(buffer string) (*commandRequest, bool, error) {
	argument1, remaining, incomplete, err := parseArgument(buffer[3:])
	if incomplete {
		return nil, true, nil
	}

	if err != nil {
		log.Println("Error with argument 1 of get command: ", err)
		return nil, false, err
	}

	if len(remaining) < 1 {
		// string too short for variable length size character to be present
		return nil, true, nil
	}

	variableLengthSizeStr := remaining[0:1]

	variableLengthSize, err := strconv.Atoi(variableLengthSizeStr)
	if err != nil {
		log.Printf("Invalid variable length size: %s", variableLengthSizeStr)
		return nil, false, err
	}

	if variableLengthSize == 0 {
		return &commandRequest{getCommand, argument1, "", 0}, false, nil
	}

	if len(remaining) < variableLengthSize+1 {
		// string too short for all of variable length argument to be present
		return nil, true, nil
	}

	variableLengthStr := remaining[1 : variableLengthSize+1]

	variableLength, err := strconv.Atoi(variableLengthStr)
	if err != nil {
		log.Printf("Invalid variable length: %s", variableLengthStr)
		return nil, false, err
	}

	return &commandRequest{getCommand, argument1, "", variableLength}, false, nil
}

func parseDeleteCommand(buffer string) (*commandRequest, bool, error) {
	argument1, _, incomplete, err := parseArgument(buffer[3:])
	if err != nil {
		log.Println("Error with argument 1 of delete command: ", err)
		return nil, false, err
	}

	if incomplete {
		return nil, true, nil
	}

	return &commandRequest{deleteCommand, argument1, "", 0}, false, nil
}

// parseArgument parses the specified string, looking for a valid 3 part argument.
// If found, the argument value is returned, along with the remaining string.
// If the parsing fails because of an invalid value (e.g. not a decimal character)
// an err is returned. If parsing fails because the string is incomplete, an incomplete
// flag is set.
//
// This implementation assumes arguments fit into an int. If data could be larger
// we could perhaps use math/big.Int.
func parseArgument(buffer string) (string, string, bool, error) {
	if len(buffer) < 3 {
		// string too short for all parts of an argument to be present
		return "", buffer, true, nil
	}

	part1String := buffer[0:1]

	argumentSizeLength, err := strconv.Atoi(part1String)
	if err != nil {
		log.Printf("Invalid part 1 of command argument: %s", part1String)
		return "", buffer, false, err
	}

	if len(buffer) < argumentSizeLength+1 {
		// string too short for all of part 2 to be present
		return "", buffer, true, nil
	}

	part2String := buffer[1 : argumentSizeLength+1]

	argumentSize, err := strconv.Atoi(part2String)
	if err != nil {
		log.Printf("Invalid part 2 of command argument: %s", part2String)
		return "", buffer, false, err
	}

	if len(buffer) < argumentSize+argumentSizeLength+1 {
		// string too short for all of part 2 to be present
		return "", buffer, true, nil
	}

	return buffer[argumentSizeLength+1 : argumentSizeLength+argumentSize+1],
		buffer[argumentSizeLength+argumentSize+1:], false, nil
}

// formatArgument outputs the specified string as a 3 part argument.
func formatArgument(input string) string {
	part3 := input
	part2 := strconv.Itoa(len(part3))
	part1 := strconv.Itoa(len(part2))

	return part1 + part2 + part3
}
