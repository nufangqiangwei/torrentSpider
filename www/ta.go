package www

import (
	"fmt"
	"github.com/allegro/bigcache/v3"
	"time"
)

func GetUser() {
	println("www:GetUser Func")
}
func test() {
	cache, _ := bigcache.NewBigCache(bigcache.DefaultConfig(10 * time.Minute))

	cache.Set("my-unique-key", []byte("value"))

	entry, _ := cache.Get("my-unique-key")
	fmt.Println(string(entry))
}
