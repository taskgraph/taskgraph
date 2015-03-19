package main
import "strings"
import "fmt"
import "errors"

func main() {
	w1 := strings.Split("a,b,c", ",")
	fmt.Println(w1[1], len(w1[1]), len(w1))
	err := errors.New("emit macho dwarf: elf header corrupted")
	fmt.Println(err)
	fmt.Printf("%T",err)
}
