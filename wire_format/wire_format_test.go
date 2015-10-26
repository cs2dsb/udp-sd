package wire_format

import  (
	"testing"
    "fmt"
	"strings"
	"strconv"
    "go/ast"
    "go/parser"
    "go/token"
	"os"
	"path/filepath"
	"io/ioutil"
	"reflect"
	"math/rand"
	"time"
)

const (
	package_path = "src/github.com/cs2dsb/udp-sd/wire_format/"
)

func getPackageFolderPath() (string, error) {
	varStrings := os.Environ()
	gopath := ""
	for _, vs := range varStrings {
		if strings.HasPrefix(strings.ToLower(vs), "gopath=") {
			parts := strings.Split(vs, "=")
			if len(parts) != 2 {
				return "", fmt.Errorf("Found gopath variable but splitting on = resulted in len != 2: %v", vs)
			}
			gopath = parts[1]
		}
	}
	
	if gopath == "" {
		return "", fmt.Errorf("Failed to find gopath from environment variables")
	}
	
	_, err := os.Stat(gopath) 
	if err != nil {
		return "", fmt.Errorf("Error stating gopath: %v", err)
	}
	
	packageRoot := filepath.Join(gopath, package_path)
	info, err := os.Stat(packageRoot)
	if err != nil {
		return "", fmt.Errorf("Error stating package root: %v", err)
	}
	
	if info.IsDir() == false {
		return "", fmt.Errorf("Package root exists but it's not a directory: %v", packageRoot)
	}
	
	return packageRoot, nil	
}

func Test_Tests_Can_List_Package_Files(t *testing.T) {
	packageRoot, err := getPackageFolderPath()
	if err != nil {
		t.Fatal(err)
	}	
	
	testFile := filepath.Join(packageRoot, "wire_format_test.go")
	_, err = os.Stat(testFile)
	if err !=  nil {
		t.Fatalf("Error statting test file: %v", err)
	}
}

func enumerateAstNodesInPackage(iterator func(filename string, node ast.Node) bool) error {
	packageRoot, err := getPackageFolderPath()
	if err != nil {
		return err
	}
	
	files, err := ioutil.ReadDir(packageRoot)
	if err != nil {
		return fmt.Errorf("Error listing package root: %v", err)
	}
	
	fileSet := token.NewFileSet()
	
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".go") {
			contents, err := ioutil.ReadFile(filepath.Join(packageRoot, f.Name()))
			if err != nil {
				return fmt.Errorf("Error reading contents of go file in package: %v", err)
			}
			
			parsed, err := parser.ParseFile(fileSet, f.Name(), contents, 0)
			if err != nil {
				return fmt.Errorf("Error parsing source file %s: %v", f.Name(), err)
			}
										
			ast.Inspect(parsed, func(n ast.Node) bool {
				return iterator(f.Name(), n)
			})
		}
	}
	
	return nil
}

func enumerateGetUniqueTokenResults(iterator func(name string, token byte) bool) error {
	var innerError error
	err := enumerateAstNodesInPackage(func(name string, n ast.Node) bool {
		if innerError != nil {
			return false
		}
		
		switch x := n.(type) {
			case *ast.FuncDecl:
				if x.Name.String() == "GetUniqueToken" {
					if len(x.Body.List) > 1 {
						innerError = fmt.Errorf("GetUniqueToken in %s has more than one statement (%d), expected just a return", name, len(x.Body.List))
						return false
					}
					rs, ok := x.Body.List[0].(*ast.ReturnStmt)
					if !ok {
						innerError = fmt.Errorf("GetUniqueToken in %s had a single statement but it wasn't a return", name)
						return false
					}
					if len(rs.Results) != 1 {
						innerError = fmt.Errorf("GetUniqueToken in %s returns %d results instead of 1", name, len(rs.Results))
						return false
					}
					vs, ok := rs.Results[0].(*ast.BasicLit)
					if !ok {
						innerError = fmt.Errorf("GetUniqueToken in %s returns something that isn't a basic literal", name)
						return false
					}
					if vs.Kind != token.INT {
						innerError = fmt.Errorf("GetUniqueToken in %s didn't return a token.INT", name)
						return false
					}
					
					asInt, err := strconv.ParseUint(vs.Value, 10, 8)
					if err != nil {
						innerError = fmt.Errorf("GetUniqueToken in %s return value error parsing as uint: %v", err)
						return false
					}
					
					b := byte(asInt)
					
					return iterator(name, b)
				}
		}
		return true
	})
	
	if err != nil {
		return err
	}
	if innerError != nil {
		return innerError
	}
	return nil
}

func Test_GetUniqueToken_Always_Unique(t *testing.T) {	
	seen := make(map[byte]bool)
	
	err := enumerateGetUniqueTokenResults(func(name string, token byte) bool {
		_, ok := seen[token]
		if ok {
			t.Errorf("GetUniqueToken in %s returns %d which we've already seen", name, token)
		}
		seen[token] = true
		return true
	})		
	
	if err != nil {
		t.Fatal(err)
	}
	
	if len(seen) == 0 {
		t.Fatalf("Didn't find any implementations of GetUniqueToken")
	}
}

func Test_Each_Unique_Serializable_Token_Has_Constructor_Registered(t *testing.T) {
	err := enumerateGetUniqueTokenResults(func(name string, token byte) bool {
		_, ok := serializableConstructors[token]
		if !ok {
			t.Errorf("Constructor missing for token %d found in file %s", token, name)
		}
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
}

func Test_Each_Serializable_Object_First_Byte_Matches_Token(t *testing.T) {
	for token, sc := range serializableConstructors {
		ob := sc.SerializableConstructor(nil)
		if ob == nil {
			t.Errorf("Nil object returned by serializable constructor for token %d", token)
			continue
		}
		
		buf := ob.Serialize()
		if len(buf) == 0 {
			t.Errorf("0 len buffer returned by serialize method for token %d", token)
			continue
		}
		
		if buf[0] != token {
			t.Errorf("First byte of buffer returned by serialize method for token %d was %d, expecting %d", token, buf[0], token)
			continue
		}
	}
}

func Test_Each_Serializable_Objects_Public_Fields_Are_Preserved(t *testing.T) {
	for token, sc := range serializableConstructors {
		newObject := sc.SerializableConstructor(nil)
		if newObject == nil {
			t.Errorf("Nil returned by serializable constructor for token %d", token)
			continue
		}
		
		ov := reflect.ValueOf(newObject).Elem()
		err := randomizeStruct(ov)
		if err != nil {
			t.Errorf("Error from randomizeStruct for token %d: %v", token, err)
			continue
		}
		
		bytes := newObject.Serialize()
		if len(bytes) == 0 {
			t.Errorf("Serialize returned 0 bytes for token %d", token)
			continue
		}
		
		sameObject := sc.SerializableConstructor(&Chunk{
			buffer: bytes,
		})
		
		if sameObject == nil {
			t.Errorf("Serializable constructor returned nil reassembling object for token %d", token)
			continue
		}
		
		if !newObject.Equals(sameObject) {
			t.Errorf("Reassembled object didn't match original for token %d: ORIGINAL: <%v>, REASSEMBLED: <%v>", token, newObject, sameObject)
			continue
		}
		
	}
}

func randomizeStruct(aStruct reflect.Value) error {
	if aStruct.Kind() != reflect.Struct {
		return fmt.Errorf("Object passed to randomizeStruct was not a structure: %v", aStruct)
	}
	
	for i := 0; i < aStruct.NumField(); i++ {
		field := aStruct.Field(i)
		fieldName := aStruct.Type().Field(i).Name
		
		if !field.CanSet() {
			continue
		}
		
		fKind := field.Kind()
		switch fKind {
			case reflect.Int64:
				field.SetInt(randomInt())

			case reflect.Float64:
			field.SetFloat(randomFloat())
			
			case reflect.String:
				field.SetString(randomString())
		
			case reflect.Struct:
				val := field.Interface()
				switch val.(type) {
					case time.Time:
						rTime := reflect.ValueOf(randomTime())
						field.Set(rTime)
					default:
						err := randomizeStruct(field)
						if err != nil {
							return err
						}
				}								
			
			default:
				return fmt.Errorf("Unsupported field type for field %s", fieldName)				
		}
	}
		
	return nil
}

func randomInt() int64 {
	return rand.Int63()
}

func randomFloat() float64 {
	return rand.Float64()
}

func randomString() string {
	return strconv.FormatInt(randomInt(), 3)
}

func randomTime() time.Time {
	return time.Unix(randomInt(), randomInt())
}




















