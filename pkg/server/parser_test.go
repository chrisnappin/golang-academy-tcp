package server

import (
	"reflect"
	"testing"
)

func Test_parseCommandBuffer_Empty(t *testing.T) {
	command, err := parseCommand("")

	checkParseCommand(t, nil, command, false, err)
}

func Test_parseCommandBuffer_Put(t *testing.T) {
	text := "put11a13foo"
	command, err := parseCommand(text)

	checkParseCommand(t, &commandRequest{putCommand, "a", "foo", 0, text}, command, false, err)
}

func Test_parseCommandBuffer_GetAll(t *testing.T) {
	text := "get11b0"
	command, err := parseCommand(text)

	checkParseCommand(t, &commandRequest{getCommand, "b", "", 0, text}, command, false, err)
}

func Test_parseCommandBuffer_GetSome(t *testing.T) {
	text := "get11b3123"
	command, err := parseCommand(text)

	checkParseCommand(t, &commandRequest{getCommand, "b", "", 123, text}, command, false, err)
}

func Test_parseCommandBuffer_Delete(t *testing.T) {
	text := "del11aww"
	command, err := parseCommand(text)

	checkParseCommand(t, &commandRequest{deleteCommand, "a", "", 0, text}, command, false, err)
}

func Test_parseCommandBuffer_Close(t *testing.T) {
	text := "bye"
	command, err := parseCommand(text)

	checkParseCommand(t, &commandRequest{closeCommand, "", "", 0, text}, command, false, err)
}

func Test_parseCommandBuffer_IncompletePut(t *testing.T) {
	command, err := parseCommand("put13aaa12b")

	checkParseCommand(t, nil, command, false, err)
}

func Test_parseCommandBuffer_IncompleteGetKey(t *testing.T) {
	command, err := parseCommand("get12a")

	checkParseCommand(t, nil, command, false, err)
}

func Test_parseCommandBuffer_IncompleteGetLengthSize(t *testing.T) {
	command, err := parseCommand("get12aa")

	checkParseCommand(t, nil, command, false, err)
}

func Test_parseCommandBuffer_IncompleteGetLength(t *testing.T) {
	command, err := parseCommand("get12aa21")

	checkParseCommand(t, nil, command, false, err)
}

func Test_parseCommandBuffer_IncompleteDelete(t *testing.T) {
	command, err := parseCommand("del4123")

	checkParseCommand(t, nil, command, false, err)
}

func Test_parseCommandBuffer_ErrorPut(t *testing.T) {
	command, err := parseCommand("put12aaX7abc")

	checkParseCommand(t, nil, command, true, err)
}

func Test_parseCommandBuffer_ErrorGetInvalidKey(t *testing.T) {
	command, err := parseCommand("get1yABC")

	checkParseCommand(t, nil, command, true, err)
}

func Test_parseCommandBuffer_ErrorGetInvalidVariablelengthSize(t *testing.T) {
	command, err := parseCommand("get13ABCx")

	checkParseCommand(t, nil, command, true, err)
}

func Test_parseCommandBuffer_ErrorGetInvalidVariablelength(t *testing.T) {
	command, err := parseCommand("get13ABC2aa")

	checkParseCommand(t, nil, command, true, err)
}

func Test_parseCommandBuffer_ErrorDelete(t *testing.T) {
	command, err := parseCommand("delQQQ")

	checkParseCommand(t, nil, command, true, err)
}

func checkParseCommand(t *testing.T, expectedCommand *commandRequest, actualCommand *commandRequest,
	isErrorExpected bool, actualErr error) {
	t.Helper()

	if isErrorExpected && actualErr == nil {
		t.Error("Error expected")
	}

	if !isErrorExpected && actualErr != nil {
		t.Error("Error not expected but got: ", actualErr)
	}

	if !reflect.DeepEqual(actualCommand, expectedCommand) {
		t.Errorf("Expected %v but got %v", expectedCommand, actualCommand)
	}
}

func Test_ParseArguments_Valid(t *testing.T) {
	argument, remaining, incomplete, err := parseArgument("212stored value..")
	if err != nil {
		t.Error("Expected successful but got: ", err)
	}

	if incomplete {
		t.Error("Expected complete argument")
	}
	checkString(t, "stored value", argument)
	checkString(t, "..", remaining)
}

func Test_ParseArguments_InvalidPart1(t *testing.T) {
	if _, _, _, err := parseArgument("x3key"); err == nil {
		t.Error("Expected error")
	}
}

func Test_ParseArguments_InvalidPart2(t *testing.T) {
	if _, _, _, err := parseArgument("2abkey"); err == nil {
		t.Error("Expected error")
	}
}

func Test_ParseArguments_AllMissing(t *testing.T) {
	argument, remaining, incomplete, err := parseArgument("12") // must be missing characters
	if err != nil {
		t.Error("Expected successful but got: ", err)
	}

	if !incomplete {
		t.Error("Expected incomplete argument")
	}

	checkString(t, "", argument)
	checkString(t, "12", remaining)
}

func Test_ParseArguments_Part2Missing(t *testing.T) {
	argument, remaining, incomplete, err := parseArgument("912345") // missing rest of digits in part 2
	if err != nil {
		t.Error("Expected successful but got: ", err)
	}

	if !incomplete {
		t.Error("Expected incomplete argument")
	}

	checkString(t, "", argument)
	checkString(t, "912345", remaining)
}

func Test_ParseArguments_Part3Missing(t *testing.T) {
	argument, remaining, incomplete, err := parseArgument("15abc") // missing rest of characters in part 3
	if err != nil {
		t.Error("Expected successful but got: ", err)
	}

	if !incomplete {
		t.Error("Expected incomplete argument")
	}

	checkString(t, "", argument)
	checkString(t, "15abc", remaining)
}

func Test_FormatArguments_Valid(t *testing.T) {
	formatted := formatArgument("key")
	checkString(t, "13key", formatted)

	formatted = formatArgument("stored value")
	checkString(t, "212stored value", formatted)
}

func checkString(t *testing.T, expected string, actual string) {
	t.Helper()

	if expected != actual {
		t.Errorf("Expected %s but got %s", expected, actual)
	}
}
