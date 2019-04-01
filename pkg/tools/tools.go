package tools

import "strings"

func AddSlashes(str string) string {
	str = strings.Replace(str, `\`, `\\`, -1)
	str = strings.Replace(str, "'", `\'`, -1)
	str = strings.Replace(str, `"`, `\"`, -1)

	return str
}

func PgEscape(origin string) (after string) {
	after = strings.Replace(origin, `'`, `''`, -1)
	return
}
