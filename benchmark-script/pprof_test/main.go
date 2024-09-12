// You can edit this code!
// Click here and start typing.
package main

import (
	"crypto/md5"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"
	"unsafe"

	"google.golang.org/api/googleapi"
)

const (
	KiB = 1024
	MiB = 1024 * KiB
)

func profileOnce(path string) (err error) {
	// Trigger a garbage collection to get up to date information (cf.
	// https://goo.gl/aXVQfL).
	//runtime.GC()

	// Open the file.
	var f *os.File
	f, err = os.Create(path)
	if err != nil {
		err = fmt.Errorf("Create: %w", err)
		return
	}

	defer func() {
		closeErr := f.Close()
		if err == nil {
			err = closeErr
		}
	}()

	// Dump to the file.
	err = pprof.Lookup("heap").WriteTo(f, 0)
	if err != nil {
		err = fmt.Errorf("WriteTo: %w", err)
		return
	}

	return
}

// func HandleMemoryProfileSignals() {
//
//		c := make(chan os.Signal, 1)
//		signal.Notify(c, syscall.SIGUSR2)
//		for range c {
//			path := fmt.Sprintf("/tmp/mem-%d.pprof", time.Now().UnixNano())
//
//			var m runtime.MemStats
//			runtime.ReadMemStats(&m)
//			fmt.Printf("Heap allocation: %d MiB", m.Alloc/MiB)
//
//			err := profileOnce(path)
//			if err == nil {
//				fmt.Printf("Wrote memory profile to %s.", path)
//			} else {
//				fmt.Printf("Error writing memory profile: %v", err)
//			}
//		}
//	}

//	type Test struct {
//		//z string
//		m map[string]int
//
// }

type ObjectAccessControlProjectTeam struct {
	// ProjectNumber: The project number.
	ProjectNumber string `json:"projectNumber,omitempty"`

	// Team: The team.
	Team string `json:"team,omitempty"`

	// ForceSendFields is a list of field names (e.g. "ProjectNumber") to
	// unconditionally include in API requests. By default, fields with
	// empty or default values are omitted from API requests. However, any
	// non-pointer, non-interface field appearing in ForceSendFields will be
	// sent to the server regardless of whether the field is empty or not.
	// This may be used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "ProjectNumber") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

// ObjectAccessControl: An access-control entry.
type ObjectAccessControl struct {
	// Bucket: The name of the bucket.
	Bucket string `json:"bucket,omitempty"`

	// Domain: The domain associated with the entity, if any.
	Domain string `json:"domain,omitempty"`

	// Email: The email address associated with the entity, if any.
	Email string `json:"email,omitempty"`

	// Entity: The entity holding the permission, in one of the following
	// forms:
	// - user-userId
	// - user-email
	// - group-groupId
	// - group-email
	// - domain-domain
	// - project-team-projectId
	// - allUsers
	// - allAuthenticatedUsers Examples:
	// - The user liz@example.com would be user-liz@example.com.
	// - The group example@googlegroups.com would be
	// group-example@googlegroups.com.
	// - To refer to all members of the Google Apps for Business domain
	// example.com, the entity would be domain-example.com.
	Entity string `json:"entity,omitempty"`

	// EntityId: The ID for the entity, if any.
	EntityId string `json:"entityId,omitempty"`

	// Etag: HTTP 1.1 Entity tag for the access-control entry.
	Etag string `json:"etag,omitempty"`

	// Generation: The content generation of the object, if applied to an
	// object.
	Generation int64 `json:"generation,omitempty,string"`

	// Id: The ID of the access-control entry.
	Id string `json:"id,omitempty"`

	// Kind: The kind of item this is. For object access control entries,
	// this is always storage#objectAccessControl.
	Kind string `json:"kind,omitempty"`

	// Object: The name of the object, if applied to an object.
	Object string `json:"object,omitempty"`

	// ProjectTeam: The project team associated with the entity, if any.
	ProjectTeam *ObjectAccessControlProjectTeam `json:"projectTeam,omitempty"`

	// Role: The access permission for the entity.
	Role string `json:"role,omitempty"`

	// SelfLink: The link to this access-control entry.
	SelfLink string `json:"selfLink,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Bucket") to
	// unconditionally include in API requests. By default, fields with
	// empty or default values are omitted from API requests. However, any
	// non-pointer, non-interface field appearing in ForceSendFields will be
	// sent to the server regardless of whether the field is empty or not.
	// This may be used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Bucket") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}
type Test struct {
	Name            string
	ContentType     string
	ContentLanguage string
	CacheControl    string
	Owner           string
	Size            uint64
	ContentEncoding string
	MD5             *[md5.Size]byte // Missing for composite objects
	CRC32C          *uint32         //Missing for CMEK buckets
	MediaLink       string
	Metadata        map[string]string
	Generation      int64
	MetaGeneration  int64
	StorageClass    string
	Deleted         time.Time
	Updated         time.Time

	// As of 2015-06-03, the official GCS documentation for this
	// property (https://goo.gl/GwD5Dq) says this:
	//
	//     Newly uploaded objects have a component count of 1, and composing a
	//     sequence of objects creates an object whose component count is equal
	//     to the sum of component counts in the sequence.
	//
	// However, in Google-internal bug 21572928 it was clarified that this
	// doesn't match the actual implementation, which can be documented as:
	//
	//     Newly uploaded objects do not have a component count. Composing a
	//     sequence of objects creates an object whose component count is equal
	//     to the sum of the component counts of the objects in the sequence,
	//     where objects that do not have a component count are treated as having
	//     a component count of 1.
	//
	// This is a much less elegant and convenient rule, so this package emulates
	// the officially documented behavior above. That is, it synthesizes a
	// component count of 1 for objects that do not have a component count.
	ComponentCount int64

	ContentDisposition string
	CustomTime         string
	EventBasedHold     bool
	Acl                []ObjectAccessControl
}

func CloneMap(m map[string]string) map[string]string {

	//return &map[string]string{strings.Clone("key1"): strings.Clone("testddddddd"),
	//	strings.Clone("key4"): strings.Clone("testdddddddffffffffffd"),
	//	strings.Clone("key2"): strings.Clone("testddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddhffffffffffffff")}
	newMap := map[string]string{}
	for k, v := range m {
		newMap[strings.Clone(k)] = strings.Clone(v)
	}

	return newMap
}
func GetObject() *Test {
	return &Test{
		Name:        strings.Clone("aa"),
		ContentType: "text/plane",
		Metadata: map[string]string{strings.Clone("key1"): strings.Clone("testddddddd"),
			strings.Clone("key2"): strings.Clone("testddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddhffffffffffffff")},
		Acl: []ObjectAccessControl{{}, {}, {}, {}, {}},
	}
}

func temp() []*Test {

	//x := "testfdddddjjghghhghfjskhkfahdkfhskjhfkeshksdjfhksjdhfksjdhfksjdhfkshdfkhsdkjfhskdjhfksjdhfkjshdfkhsdkfjhskjdhfkjshdfksjhdfkjshdjkfhs"
	//x = x + "testfdddddjjghghhghfjskhkfahdkfhskjhfkeshksdjfhksjdhfksjdhfksjdhfkshdfkhsdkjfhskdjhfksjdhfkjshdfkhsdkfjhskjdhfkjshdfksjhdfkjshdjkfhs"

	//seed := 1000000
	//strs := []string{}
	//for i := 0; i < 2e5; i++ {
	//
	//	y := strconv.Itoa(seed + i)
	//	strs = append(strs, y)
	//}

	//return strs
	test := []*Test{}
	for i := 0; i < 1; i++ {
		//test1 := &Test{"tkjdfkjsdlfkkkkkkkkkkkkkkkkkkkkkrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk"}
		//test1.z = strconv.Itoa(seed + i)
		//test1 := &Test{z: strings.Clone("aaaaaaaaaaaaaaaa")}

		test1 := GetObject()

		test2 := CloneMap(test1.Metadata)
		clone2 := CloneMap(test1.Metadata)
		clone3 := CloneMap(test1.Metadata)
		fmt.Println(len(test2))
		fmt.Println(len(clone2))
		fmt.Println(len(clone3))
		test = append(test, test1)
	}
	fmt.Printf("Object ACL: %v\n", unsafe.Sizeof(ObjectAccessControl{}))
	return test
}
func main() {
	//runtime.GC()
	runtime.MemProfileRate = 1
	x := temp()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	//fmt.Printf("Heap allocation: %+v ", m)
	fmt.Printf("Heap object: %d ", m.HeapObjects)
	fmt.Printf("Heap in use: %+v ", m.HeapInuse/MiB)
	fmt.Println(len(x))
	//go temp()
	profileOnce("mem.pprof")

}
