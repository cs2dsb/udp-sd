package wire_format

import  (
	"testing"
    "fmt"
	"strings"
	"strconv"
    "go/ast"
    //"go/format"
    "go/parser"
    "go/token"
	"os"
	"path/filepath"
	"io/ioutil"
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

func Test_GetUniqueToken_Always_Unique(t *testing.T) {	
	packageRoot, err := getPackageFolderPath()
	if err != nil {
		t.Fatal(err)
	}
	
	files, err := ioutil.ReadDir(packageRoot)
	if err != nil {
		t.Fatalf("Error listing package root: %v", err)
	}
	
	fileSet := token.NewFileSet()
	
	seen := make(map[byte]bool)
	
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".go") {
			contents, err := ioutil.ReadFile(filepath.Join(packageRoot, f.Name()))
			if err != nil {
				t.Fatalf("Error reading contents of go file in package: %v", err)
			}
			
			parsed, err := parser.ParseFile(fileSet, f.Name(), contents, 0)
			if err != nil {
				t.Fatalf("Error parsing source file %s: %v", f.Name(), err)
			}
						
			ast.Inspect(parsed, func(n ast.Node) bool {
				switch x := n.(type) {
					case *ast.FuncDecl:
						if x.Name.String() == "GetUniqueToken" {
							if len(x.Body.List) > 1 {
								t.Fatalf("GetUniqueToken in %s has more than one statement (%d), expected just a return", f.Name(), len(x.Body.List))
							}
							rs, ok := x.Body.List[0].(*ast.ReturnStmt)
							if !ok {
								t.Fatalf("GetUniqueToken in %s had a single statement but it wasn't a return", f.Name())
							}
							if len(rs.Results) != 1 {
								t.Fatalf("GetUniqueToken in %s returns %d results instead of 1", f.Name(), len(rs.Results))
							}
							vs, ok := rs.Results[0].(*ast.BasicLit)
							if !ok {
								t.Fatalf("GetUniqueToken in %s returns something that isn't a basic literal", f.Name())
							}
							if vs.Kind != token.INT {
								t.Fatalf("GetUniqueToken in %s didn't return a token.INT", f.Name())
							}
							
							asInt, err := strconv.ParseUint(vs.Value, 10, 8)
							if err != nil {
								t.Fatalf("GetUniqueToken in %s return value error parsing as uint: %v", err)
							}
							
							b := byte(asInt)
							_, ok = seen[b]
							if ok {
								t.Errorf("GetUniqueToken in %s returns %d which we've already seen", f.Name(), b)
							}
							seen[b] = true
						}
				}
				return true
			})
		}
	}
	
	if len(seen) == 0 {
		t.Fatalf("Didn't find any implementations of GetUniqueToken")
	}
}






















