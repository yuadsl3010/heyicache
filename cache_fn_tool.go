package heyicache

import (
	"fmt"
	"os"
	"reflect"
	"runtime"
	"strings"
)

var (
	gitRepo         = "github.com/yuadsl3010"
	gitPkgName      = "heyicache"
	sliceHeaderSize = "size += int32(unsafe.Sizeof(int32(0)))"
	prefix          = "HeyiCacheFn"
	structSize      = prefix + "StructSize"
	funcGet         = prefix + "Get"
	funcSize        = prefix + "Size"
	funcSet         = prefix + "Set"
)

// you can use this tool to generate the function Set(), Get() and Size()
func GenCacheFn(obj interface{}, isMainPkgStruct bool) {
	t := reflect.TypeOf(obj)
	if t.Kind() != reflect.Struct {
		fmt.Println("only support struct type")
		return
	}

	// caller package name
	callerPkg := ""
	callerPkgName := ""
	pc, _, _, ok := runtime.Caller(1)
	if ok {
		fn := runtime.FuncForPC(pc)
		if fn != nil {
			fmt.Println("caller fn name:", fn.Name())
			callerPkg, callerPkgName = getPackageFromFuncName(fn.Name(), isMainPkgStruct)
			fmt.Println("callerPkg:", callerPkg, "callerPkgName:", callerPkgName)
		}
	}

	typeInfos := collectSubStructPackages(t, make(map[string]*TypeInfo))
	fmt.Println("sub struct type infos:")
	for _, info := range typeInfos {
		genCacheFn(info.T, callerPkg, callerPkgName, isMainPkgStruct, info.GetPkgs())
	}
}

func genCacheFn(t reflect.Type, callerPkg string, callerPkgName string, isMainPkgStruct bool, subPkgs []string) {
	// reverse struct type to get field tools
	fieldTools := getFieldTools(t)

	// start writing code
	// package
	// import
	ct := NewCodeTool(t.Name(), callerPkg, callerPkgName, isMainPkgStruct, t.PkgPath(), subPkgs)
	defer ct.Done()
	fmt.Println("name", t.Name())
	fmt.Println("pkg", t.PkgPath())

	// var
	structName := t.Name()                        // Store
	fullStructName := getFullTypeName(t.String()) // *o2oalgo.Store
	fmt.Println("structName", structName)
	fmt.Println("fullStructName", fullStructName)
	if strings.HasPrefix(fullStructName, fmt.Sprintf("%v.", callerPkgName)) {
		fullStructName = structName
	}

	genCacheFnStructSize(ct, structName, fullStructName)

	// func Get()
	genCacheFnGet(ct, structName, fullStructName)

	// func Size()
	genCacheFnSize(ct, structName, fullStructName, fieldTools)

	// func Set()
	genCacheFnSet(ct, structName, fullStructName, fieldTools)
}

func getFieldTools(t reflect.Type) []*FieldTool {
	fieldTools := make([]*FieldTool, 0, t.NumField())
	// fields
	for i := 0; i < t.NumField(); i++ {
		skip := false
		field := t.Field(i)
		// opts from tags
		for _, opt := range strings.Split(field.Tag.Get("heyicache"), ",") {
			switch opt {
			case "skip":
				skip = true
			}
		}

		fieldTools = append(fieldTools, &FieldTool{
			Name:       field.Name,
			TypeName:   field.Type.Name(),
			Type:       field.Type,
			IsExported: field.IsExported(),
			IsSkip:     skip,
		})
	}
	return fieldTools
}

func getFullTypeName(name string) string {
	if after, ok := strings.CutPrefix(name, "main."); ok {
		name = after
	}
	if after, ok := strings.CutPrefix(name, "*main."); ok {
		name = "*" + after
	}
	return name
}

func genCacheFnStructSize(ct *CodeTool, structName, fullStructName string) {
	// size
	ct.Println("var (")
	ct.In()
	{
		ct.Println("// pass this ifc_ to heyicache in Get/Set")
		ct.Println(ct.getFnIfc_(structName) + " = &" + ct.getFnIfc(structName) + "{")
		{
			ct.In()
			ct.Println("StructSize: int(unsafe.Sizeof(" + fullStructName + "{})),")
			ct.Out()
		}
		ct.Println("}")
	}
	ct.Out()
	ct.Println(")")
	ct.Println("")

	ct.Println("type " + ct.getFnIfc(structName) + " struct {")
	{
		ct.In()
		ct.Println("StructSize int")
	}
	ct.Out()
	ct.Println("}")
	ct.Println("")
}

func genCacheFnGet(ct *CodeTool, structName, fullStructName string) {
	ct.Println("func (ifc *" + ct.getFnIfc(structName) + ") Get (bs []byte) interface{} {")
	ct.In()
	ct.Println("if len(bs) == 0 || len(bs) < ifc.StructSize {")
	{
		ct.In()
		ct.Println("return nil")
		ct.Out()
	}
	ct.Println("}")
	ct.Println("")
	ct.Println("return (*" + fullStructName + ")(unsafe.Pointer(&bs[0]))")
	ct.Out()
	ct.Println("}")
	ct.Println("")
}

func genCacheFnSize(ct *CodeTool, structName, fullStructName string, fieldTools []*FieldTool) {
	needObj := false
	ct.Println("func (ifc *" + ct.getFnIfc(structName) + ") Size (value interface{}, isStructPtr bool) int32 {")
	ct.In()
	ct.CheckPtrNil(needObj)
	ct.CheckPtr(fullStructName, needObj)

	ct.Println("var size int32")
	ct.Println("if isStructPtr {")
	{
		ct.In()
		// foo *Foo
		ct.Println("size = int32(ifc.StructSize)")

		ct.Out()
	}
	ct.Println("}")

	// foo.A, foo.B, etc.
	ct.SizeStructFields(fieldTools)

	ct.Println("return size")
	ct.Out()
	ct.Println("}")
	ct.Println("")
}

func genCacheFnSet(ct *CodeTool, structName, fullStructName string, fieldTools []*FieldTool) {
	needObj := true
	ct.Println("func (ifc *" + ct.getFnIfc(structName) + ") Set (value interface{}, bs []byte, isStructPtr bool) (interface{}, int32) {")
	ct.In()
	ct.CheckPtrNil(needObj)
	ct.CheckPtr(fullStructName, needObj)

	ct.Println("dst := src")
	ct.Println("var size int32")
	ct.Println("if isStructPtr {")
	{
		ct.In()
		// foo *Foo
		ct.Println("size = int32(ifc.StructSize)")
		ct.Println("srcBytes := (*[1 << 30]byte)(unsafe.Pointer(src))[:size:size]")
		ct.Println("copy(bs, srcBytes)")
		ct.Println("dst = (*" + fullStructName + ")(unsafe.Pointer(&bs[0]))")
		ct.Out()
	}
	ct.Println("}")

	// foo.A, foo.B, etc.
	ct.SetStructFields(fieldTools)

	ct.Println("return dst, size")
	ct.Out()
	ct.Println("}")
	ct.Println("")
}

type CodeTool struct {
	count           int
	header          strings.Builder
	content         strings.Builder
	filename        string
	callerPkg       string
	callerPkgName   string
	isMainPkgStruct bool
	objPkg          string
	subPkgs         []string
	needImport      bool
}

func NewCodeTool(name, callerPkg, callerPkgName string, isMainPkgStruct bool, objPkg string, subPkgs []string) *CodeTool {
	// Extract the last part of the package name
	pkgParts := strings.Split(objPkg, "/") // eg: github.com/yuadsl3010/heyicache/o2oalgo
	objPkgName := pkgParts[len(pkgParts)-1]

	// Generate filename: heyicache_fn_{pkgName}_{name}.go
	filename := fmt.Sprintf("heyicache_fn_%s_%s.go",
		strings.ToLower(objPkgName),
		strings.ToLower(name))

	ct := &CodeTool{
		filename:        filename,
		callerPkg:       callerPkg,
		callerPkgName:   callerPkgName,
		isMainPkgStruct: isMainPkgStruct,
		objPkg:          objPkg,
		subPkgs:         subPkgs,
	}

	return ct
}

func (ct *CodeTool) writeHeader() {
	ct.header.WriteString(fmt.Sprintf("package %v\n\n", ct.callerPkgName))
	ct.header.WriteString("import (\n")
	ct.header.WriteString("\t\"unsafe\"\n")
	ct.header.WriteString("\n")
	if ct.needImport {
		ct.header.WriteString("\t\"" + gitRepo + "/" + gitPkgName + "\"\n")
	}
	// Use map to remove duplicates
	uniquePkgs := make(map[string]struct{})
	uniquePkgs[ct.objPkg] = struct{}{}
	for _, subPkg := range ct.subPkgs {
		uniquePkgs[subPkg] = struct{}{}
	}
	for pkg := range uniquePkgs {
		if strings.HasSuffix(pkg, fmt.Sprintf("/%v", ct.callerPkgName)) {
			continue
		}
		if ct.isMainPkgStruct && ct.callerPkg == pkg {
			continue
		}
		ct.header.WriteString(fmt.Sprintf("\t\"%s\"\n", pkg))
	}
	ct.header.WriteString(")\n\n")
}

func (ct *CodeTool) Done() {
	// write to file
	ct.writeHeader()
	ct.header.WriteString(ct.content.String())
	err := os.WriteFile(ct.filename, []byte(ct.header.String()), 0644)
	if err != nil {
		fmt.Printf("Error writing file %s: %v\n", ct.filename, err)
		return
	}
	fmt.Printf("Generated file: %s\n", ct.filename)
}

func (ct *CodeTool) In() {
	ct.count++
}

func (ct *CodeTool) Out() {
	ct.count--
}

func (ct *CodeTool) Println(str string) {
	data := ""
	for i := 0; i < ct.count; i++ {
		data += "\t"
	}

	ct.content.WriteString(data + str + "\n")
}

func (ct *CodeTool) getFnIfc(typeName string) string {
	return prefix + typeName + "Ifc"
}

func (ct *CodeTool) getFnIfc_(typeName string) string {
	return ct.getFnIfc(typeName) + "_"
}

func (ct *CodeTool) getStructSize(typeName string) string {
	return structSize + typeName
}

func (ct *CodeTool) getFuncGet(typeName string) string {
	// HeyiCacheFnGetStore
	return funcGet + typeName
}

func (ct *CodeTool) getFuncSize(typeName string) string {
	// HeyiCacheFnSizeStore
	return funcSize + typeName
}

func (ct *CodeTool) getFuncSet(typeName string) string {
	// HeyiCacheFnSetStore
	return funcSet + typeName
}

func (ct *CodeTool) getFuncSizeSlice() string {
	// heyicache.HeyiCacheFnSizeSlice
	ct.needImport = true
	return gitPkgName + "." + funcSize + "Slice"
}

func (ct *CodeTool) getFuncSetSlice() string {
	// heyicache.HeyiCacheFnSetSlice
	ct.needImport = true
	return gitPkgName + "." + funcSet + "Slice"
}

func (ct *CodeTool) getFuncSizeString() string {
	// heyicache.HeyiCacheFnSizeString
	ct.needImport = true
	return gitPkgName + "." + funcSize + "String"
}

func (ct *CodeTool) getFuncSetString() string {
	// heyicache.HeyiCacheFnSetString
	ct.needImport = true
	return gitPkgName + "." + funcSet + "String"
}

func (ct *CodeTool) getFuncStructSize(typeName string) string {
	// heyicache.HeyiCacheFnStructSizeStore
	ct.needImport = true
	return gitPkgName + "." + structSize + typeName
}

func (ct *CodeTool) CheckPtr(fullStructName string, returnObj bool) {
	ct.Println("src, ok := value.(*" + fullStructName + ")")
	ct.Println("if !ok || src == nil {")
	{
		ct.In()
		ct.PrintReturn(returnObj)
		ct.Out()
	}
	ct.Println("}")
	ct.Println("")
}

func (ct *CodeTool) PrintReturn(returnObj bool) {
	if returnObj {
		ct.Println("return nil, 0")
	} else {
		ct.Println("return 0")
	}
}

func (ct *CodeTool) CheckPtrNil(returnObj bool) {
	ct.Println("if value == nil {")
	{
		ct.In()
		ct.PrintReturn(returnObj)
		ct.Out()
	}
	ct.Println("}")
	ct.Println("")
}

func (ct *CodeTool) CheckSize() {
	return
	ct.Println("if size >= int32(maxLen) {")
	ct.In()
	ct.Println("// fail because of the buffer size, try again after eviction")
	ct.Println("return nil, size")
	ct.Out()
	ct.Println("}")
	ct.Println("")
}

func (ct *CodeTool) SetSlice(name, typeName string) {
	ct.Println("p" + name + ", size" + name + " := " + ct.getFuncSetSlice() + "(src." + name + ", bs[size:], " + ct.getFuncStructSize(typeName) + ")")
	ct.Println("size += size" + name)
	ct.CheckSize()
	ct.Println("dst." + name + " = p" + name)
}

func (ct *CodeTool) SetSliceCustom(name, typeName string) {
	ct.Println("p" + name + ", size" + name + " := " + ct.getFuncSetSlice() + "(src." + name + ", bs[size:], " + ct.getFnIfc_(typeName) + ".StructSize)")
	ct.Println("size += size" + name)
	ct.CheckSize()
	ct.Println("dst." + name + " = p" + name)
}

func (ct *CodeTool) setStruct(name, typeName, item string) {
	ct.Println("_, size" + name + " := " + ct.getFnIfc_(typeName) + ".Set(" + item + ", bs[size:], false)")
	ct.Println("size += size" + name)
	ct.CheckSize()
}

func (ct *CodeTool) SetStruct(name, typeName string) {
	ct.setStruct(name, typeName, "&dst."+name)
}

func (ct *CodeTool) SetSliceStruct(name, typeName string) {
	ct.Println("for idx := range dst." + name + " {")
	ct.In()
	ct.setStruct(name, typeName, "&dst."+name+"[idx]")
	ct.Out()
	ct.Println("}")
}

func (ct *CodeTool) setStructPtr(name, typeName, fullName, item, idx string) {
	ct.Println("p" + name + ", size" + name + " := " + ct.getFnIfc_(typeName) + ".Set(" + item + ", bs[size:], true)")
	ct.Println("size += size" + name)
	ct.CheckSize()
	ct.Println("if p" + name + " != nil && size" + name + " > 0 {")
	ct.In()
	ct.Println("dst." + name + idx + " = p" + name + ".(" + fullName + ")")
	ct.Out()
	ct.Println("}")
	ct.Println("")
}

func (ct *CodeTool) SetStructPtr(name, typeName, fullName string) {
	ct.setStructPtr(name, typeName, fullName, "src."+name, "")
}

func (ct *CodeTool) SetSliceStructPtr(name, typeName, fullName string) {
	ct.Println("for idx, item := range src." + name + " {")
	ct.In()
	ct.setStructPtr(name, typeName, fullName, "item", "[idx]")
	ct.Out()
	ct.Println("}")
}

func (ct *CodeTool) setString(name, item, idx string) {
	ct.Println("p" + name + ", size" + name + " := " + ct.getFuncSetString() + "(" + item + ", bs[size:])")
	ct.Println("size += size" + name)
	ct.CheckSize()
	ct.Println("dst." + name + idx + " = p" + name)
}

func (ct *CodeTool) SetString(name string) {
	ct.setString(name, "src."+name, "")
}

func (ct *CodeTool) SetSliceString(name string) {
	ct.Println("for idx, item := range src." + name + " {")
	ct.In()
	ct.setString(name, "item", "[idx]")
	ct.Out()
	ct.Println("}")
}

func (ct *CodeTool) SizeSlice(name, typeName string) {
	ct.Println("size += " + ct.getFuncSizeSlice() + "(src." + name + ", " + ct.getFuncStructSize(typeName) + ")")
}

func (ct *CodeTool) SizeSliceCustom(name, typeName string) {
	ct.Println("size += " + ct.getFuncSizeSlice() + "(src." + name + ", " + ct.getFnIfc_(typeName) + ".StructSize)")

}

func (ct *CodeTool) sizeString(item string) {
	ct.Println("size += " + ct.getFuncSizeString() + "(" + item + ")")
}

func (ct *CodeTool) SizeString(name string) {
	ct.sizeString("src." + name)
}

func (ct *CodeTool) SizeSliceString(name string) {
	ct.Println("for _, item := range src." + name + " {")
	ct.In()
	ct.sizeString("item")
	ct.Out()
	ct.Println("}")
}

func (ct *CodeTool) sizeStruct(typeName, item string) {
	ct.Println("size += " + ct.getFnIfc_(typeName) + ".Size(" + item + ", false)")
}

func (ct *CodeTool) SizeStruct(name, typeName string) {
	ct.sizeStruct(typeName, "&src."+name)
}

func (ct *CodeTool) SizeSliceStruct(name, typeName string) {
	ct.Println("for idx := range src." + name + " {")
	ct.In()
	ct.sizeStruct(typeName, "&src."+name+"[idx]")
	ct.Out()
	ct.Println("}")
}

func (ct *CodeTool) sizeStructPtr(typeName, item string) {
	ct.Println("size += " + ct.getFnIfc_(typeName) + ".Size(" + item + ", true)")
}

func (ct *CodeTool) SizeStructPtr(name, typeName string) {
	ct.sizeStructPtr(typeName, "src."+name)
}

func (ct *CodeTool) SizeSliceStructPtr(name, typeName string) {
	ct.Println("for _, item := range src." + name + " {")
	ct.In()
	ct.sizeStructPtr(typeName, "item")
	ct.Out()
	ct.Println("}")
}

func (ct *CodeTool) SetStructFields(fieldTools []*FieldTool) {
	for _, field := range fieldTools {
		ct.Println(field.Check())
		if field.IsSkip || field.Category == FieldTypeNotStructPtr || field.Category == FieldTypeMap {
			// skip field
			ct.Println("// skip field: " + field.Name)
			prefix := ""
			if field.IsSkip {
				// foo *map[string]string
				// foo map[string]string, map[int]int, etc. (not supported in this tool, but can be used in custom serialization)
				prefix = "// "
			}

			ct.Println(prefix + "dst." + field.Name + " = nil")
			continue
		}

		switch field.Category {
		case FieldTypeStruct: // foo Foo
			ct.Println("// struct: foo Foo")
			ct.SetStruct(field.Name, field.TypeName)
		case FieldTypeStructPtr: // foo *Foo
			ct.Println("// struct ptr: foo *Foo")
			ct.SetStructPtr(field.Name, field.TypeName, field.FullTypeName)
		case FieldTypeString: // foo string
			ct.Println("// string: foo string")
			ct.SetString(field.Name)
		case FieldTypeSliceStruct: // foo []Foo
			ct.Println("// slice struct: foo []Foo")
			ct.SetSliceCustom(field.Name, field.TypeName)
			ct.SetSliceStruct(field.Name, field.TypeName)
		case FieldTypeSliceStructPtr: // foo []*Foo
			ct.Println("// slice struct ptr: foo []*Foo")
			ct.SetSlice(field.Name, "ptr")
			ct.SetSliceStructPtr(field.Name, field.TypeName, field.FullTypeName)
		case FieldTypeSliceString: // foo []string
			ct.Println("// slice string: foo []string")
			ct.SetSlice(field.Name, field.TypeName)
			ct.SetSliceString(field.Name)
		case FieldTypeSlice: // foo []int, []byte, etc.
			ct.Println("// slice: foo []int, []byte, etc.")
			ct.SetSlice(field.Name, field.TypeName)
		}
	}
	ct.Println("")
}

func (ct *CodeTool) SizeStructFields(fieldTools []*FieldTool) {
	for _, field := range fieldTools {
		ct.Println(field.Check())
		if field.IsSkip || field.Category == FieldTypeNotStructPtr || field.Category == FieldTypeMap {
			ct.Println("// skip field: " + field.Name)
			continue
		}
		switch field.Category {
		case FieldTypeStruct: // foo Foo
			ct.Println("// struct: foo Foo")
			ct.SizeStruct(field.Name, field.TypeName)
		case FieldTypeStructPtr: // foo *Foo
			ct.Println("// struct ptr: foo *Foo")
			ct.SizeStructPtr(field.Name, field.TypeName)
		case FieldTypeString: // foo string
			ct.Println("// string: foo string")
			ct.SizeString(field.Name)
		case FieldTypeSliceStruct: // foo []Foo
			ct.Println("// slice struct: foo []Foo")
			ct.SizeSliceCustom(field.Name, field.TypeName)
			ct.SizeSliceStruct(field.Name, field.TypeName)
		case FieldTypeSliceStructPtr: // foo []*Foo
			ct.Println("// slice struct ptr: foo []*Foo")
			ct.SizeSlice(field.Name, "ptr")
			ct.SizeSliceStructPtr(field.Name, field.TypeName)
		case FieldTypeSliceString: // foo []string
			ct.Println("// slice string: foo []string")
			ct.SizeSlice(field.Name, field.TypeName)
			ct.SizeSliceString(field.Name)
		case FieldTypeSlice: // foo []int, []byte, etc.
			ct.Println("// slice: foo []int, []byte, etc.")
			ct.SizeSlice(field.Name, field.TypeName)
		}
	}
}

type FieldType int

const (
	FieldTypeUnknown        FieldType = iota
	FieldTypeStruct         FieldType = 1 // foo Foo
	FieldTypeStructPtr      FieldType = 2 // foo *Foo
	FieldTypeNotStructPtr   FieldType = 3 // foo *map[string]string
	FieldTypeSliceStruct    FieldType = 4 // foo []Foo
	FieldTypeSliceStructPtr FieldType = 5 // foo []*Foo
	FieldTypeSliceString    FieldType = 6 // foo []string
	FieldTypeSlice          FieldType = 7 // foo []int, []byte, etc.
	FieldTypeMap            FieldType = 8 // foo map[string]string, map[int]int, etc. (not supported in this tool, but can be used in custom serialization)
	FieldTypeString         FieldType = 9 // foo string
)

//	type TestFoo struct {
//		A1 *TestFoo
//	}
type FieldTool struct {
	Name         string // A1
	TypeName     string // TestFoo
	FullTypeName string // *heyicache.TestFoo, only use for *Struct or []*Struct
	Type         reflect.Type

	IsExported bool
	IsSkip     bool

	Category FieldType
}

func (ft *FieldTool) Check() string {
	status := "success"

	t := ft.Type
	switch t.Kind() {
	case reflect.Slice, reflect.Array:
		switch t.Elem().Kind() {
		case reflect.Ptr:
			name := t.Elem().String() // *heyicache.TestFoo
			names := strings.Split(name, ".")
			ft.TypeName = names[len(names)-1] // TestFoo
			ft.FullTypeName = getFullTypeName(name)
			ft.Category = FieldTypeSliceStructPtr
		case reflect.Struct:
			name := t.Elem().String() // heyicache.TestFoo
			names := strings.Split(name, ".")
			ft.TypeName = names[len(names)-1] // TestFoo
			ft.Category = FieldTypeSliceStruct
		case reflect.String:
			ft.TypeName = t.Elem().String() // int, uint, float32, etc.
			ft.Category = FieldTypeSliceString
		case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128:
			ft.TypeName = t.Elem().String() // int, uint, float32, etc.
			ft.Category = FieldTypeSlice
		default:
			ft.Category = FieldTypeNotStructPtr
		}
	case reflect.Ptr:
		switch t.Elem().Kind() {
		case reflect.Struct:
			ft.TypeName = t.Elem().Name()                       // TestFoo
			ft.FullTypeName = getFullTypeName(ft.Type.String()) // *heyicache.TestFoo
			ft.Category = FieldTypeStructPtr
		default:
			ft.Category = FieldTypeNotStructPtr
		}
	case reflect.Struct:
		ft.TypeName = t.Name()
		ft.Category = FieldTypeStruct
	case reflect.String:
		ft.Category = FieldTypeString
	case reflect.Map:
		ft.Category = FieldTypeMap
	}

	switch ft.Category {
	case FieldTypeNotStructPtr:
		status = "error! pointer type must point to struct"
	case FieldTypeMap:
		status = "skip and set nil! map type not supported cause it can't be stored by value, you must use custom serlization to store it if you really want map"
	}

	if ft.IsSkip {
		status = "skip and set nil! struct tag skip"
	}

	if !ft.IsExported {
		status = "not exported, it can't be used after get from cache"
	}

	return fmt.Sprintf("// %v: %v", ft.Name, status)
}

func getPackageFromFuncName(funcName string, isMainPkgStruct bool) (string, string) {
	// input github.com/yuadsl3010/heyicache-benchmark.TestFnGenerateTool, true
	// output github.com/yuadsl3010/heyicache-benchmark, ""
	// input github.com/yuadsl3010/heyicache-benchmark/codec.TestFnGenerateTool, false
	// output github.com/yuadsl3010/heyicache-benchmark/codec, codec
	// fmt.Println("funcName:", funcName, "isMainPkgStruct:", isMainPkgStruct)
	pkg := ""
	pkgName := ""
	if idx := strings.LastIndex(funcName, "/"); idx >= 0 {
		pkg = funcName[:idx]
		pkgName = funcName[idx+1:]
		if idx := strings.LastIndex(pkgName, "."); idx >= 0 {
			pkgName = pkgName[:idx]
			pkg = pkg + "/" + pkgName
		}
		if isMainPkgStruct {
			pkgName = "main"
		}
	}
	// fmt.Println("pkg:", pkg, "pkgName:", pkgName)
	return pkg, pkgName
}

type TypeInfo struct {
	T    reflect.Type
	Pkgs map[string]struct{}
}

func (a *TypeInfo) Add(ss []*TypeInfo) {
	for _, b := range ss {
		for pkg := range b.Pkgs {
			a.Pkgs[pkg] = struct{}{}
		}
	}
}

func (a *TypeInfo) GetPkgs() []string {
	pkgs := make([]string, 0, len(a.Pkgs))
	for pkg := range a.Pkgs {
		pkgs = append(pkgs, pkg)
	}
	return pkgs
}

// collectSubStructPackages recursively collects all sub struct package paths
// collectSubStructPackages recursively collects all sub struct types and package names
func collectSubStructPackages(t reflect.Type, visited map[string]*TypeInfo) []*TypeInfo {
	var typeInfos []*TypeInfo
	typeName := t.String()
	if info, exists := visited[typeName]; exists {
		if info != nil {
			return []*TypeInfo{info}
		}
		return typeInfos
	}
	// Mark as visiting to prevent infinite recursion
	visited[typeName] = nil
	if t.Kind() != reflect.Struct {
		return typeInfos
	}
	// Current struct type
	var currentTypeInfo *TypeInfo
	if t.PkgPath() != "" {
		currentTypeInfo = &TypeInfo{
			T:    t,
			Pkgs: map[string]struct{}{t.PkgPath(): {}},
		}
		typeInfos = append(typeInfos, currentTypeInfo)
		visited[typeName] = currentTypeInfo
	}
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fieldType := field.Type
		switch fieldType.Kind() {
		case reflect.Struct:
			subTypeInfos := collectSubStructPackages(fieldType, visited)
			currentTypeInfo.Add(subTypeInfos)
			typeInfos = append(typeInfos, subTypeInfos...)
		case reflect.Ptr:
			if fieldType.Elem().Kind() == reflect.Struct {
				elemType := fieldType.Elem()
				subTypeInfos := collectSubStructPackages(elemType, visited)
				currentTypeInfo.Add(subTypeInfos)
				typeInfos = append(typeInfos, subTypeInfos...)
			}
		case reflect.Slice, reflect.Array:
			elemType := fieldType.Elem()
			if elemType.Kind() == reflect.Struct {
				subTypeInfos := collectSubStructPackages(elemType, visited)
				currentTypeInfo.Add(subTypeInfos)
				typeInfos = append(typeInfos, subTypeInfos...)
			} else if elemType.Kind() == reflect.Ptr && elemType.Elem().Kind() == reflect.Struct {
				structType := elemType.Elem()
				subTypeInfos := collectSubStructPackages(structType, visited)
				currentTypeInfo.Add(subTypeInfos)
				typeInfos = append(typeInfos, subTypeInfos...)
			}
		}
	}
	// Remove duplicates
	unique := make(map[string]*TypeInfo)
	for _, info := range typeInfos {
		if info != nil {
			unique[info.T.String()] = info
		}
	}
	result := make([]*TypeInfo, 0, len(unique))
	for _, info := range unique {
		result = append(result, info)
	}
	return result
}
