package main
import "strings"
import "fmt"
// import "errors"
import "regexp"

func main() {
	w1 := strings.Split("a,b,c", ",")
	fmt.Println(w1[1], len(w1[1]), len(w1))
	// err := errors.New("emit macho dwarf: elf header corrupted")
	// fmt.Println(err)
	// fmt.Printf("%T",err)
	match, err := regexp.MatchString(".*", "test11111")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(match)
}