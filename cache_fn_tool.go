package heyicache

import (
	"fmt"
	"go/token"
	"os"
	"reflect"
	"runtime"
	"strings"
)

// skill_name: development; plugin_name: development; skill_branch: main;
// skill_commit: 125eeb496930a08d171acf2dbc7f65f1aa3c4f74;
// skill_updated_at: 2026-09-30T15:53:35+08:00; skill_version_status: unknown

var (
	gitRepo         = "github.com/yuadsl3010"
	gitPkgName      = "heyicache"
	sliceHeaderSize = "size += int32(unsafe.Sizeof(int32(0)))"
	prefix          = "HeyiCacheFn"
	structSize      = prefix + "StructSize"
	funcGet         = prefix + "Get"
	funcSize        = prefix + "Size"
	funcSet         = prefix + "Set"
	warn            = "[HeyiCacheWarn] "
)

func GenCacheFn(obj interface{}) {
	genCacheFn(obj, false)
}

// if your package is main, use this to generate the code
func GenCacheFnIsMainPkgStruct(obj interface{}) {
	genCacheFn(obj, true)
}

// you can use this tool to generate the function Set(), Get() and Size()
func genCacheFn(obj interface{}, isMainPkgStruct bool) {
	t := reflect.TypeOf(obj)
	if t.Kind() != reflect.Struct {
		fmt.Println("only support struct type")
		return
	}

	// caller package name
	callerPkg := ""
	callerPkgName := ""
	pc, _, _, ok := runtime.Caller(2)
	if ok {
		fn := runtime.FuncForPC(pc)
		if fn != nil {
			fmt.Println("caller fn name:", fn.Name())
			callerPkg, callerPkgName = getPackageFromFuncName(fn.Name(), isMainPkgStruct)
			fmt.Println("callerPkg:", callerPkg, "callerPkgName:", callerPkgName)
		}
	}

	typeInfos := collectSubStructPackages(t, make(map[string]*TypeInfo))
	// Validate the whole graph before emitting any files.
	for _, info := range typeInfos {
		validateCacheType(info.T, callerPkg)
	}
	fmt.Println("sub struct type infos:")
	for _, info := range typeInfos {
		xxx_genCacheFn(info.T, callerPkg, callerPkgName, isMainPkgStruct, info.GetPkgs())
	}

	// ANSI color codes: \033[1;31m = bold red, \033[1;33m = bold yellow, \033[0m = reset
	fmt.Printf("\n"+
		"\033[1;33m╔═══════════════════════════════════════════════════════════════════╗\033[0m\n"+
		"\033[1;33m║\033[0m\033[1;31m                         ⚠️  IMPORTANT  ⚠️                           \033[0m\033[1;33m║\033[0m\n"+
		"\033[1;33m╠═══════════════════════════════════════════════════════════════════╣\033[0m\n"+
		"\033[1;33m║\033[0m  \033[1;36mPlease search for \"\033[1;31m%s\033[1;36m\" in the generated files\033[0m       \033[1;33m║\033[0m\n"+
		"\033[1;33m║\033[0m  \033[1;36mand carefully review ALL warning messages before using!\033[0m          \033[1;33m║\033[0m\n"+
		"\033[1;33m╚═══════════════════════════════════════════════════════════════════╝\033[0m\n", strings.TrimSpace(warn))
}

func xxx_genCacheFn(t reflect.Type, callerPkg string, callerPkgName string, isMainPkgStruct bool, subPkgs []string) {
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
	genCacheFnProject(ct, structName, fullStructName, fieldTools)

	// func New()
	genCacheFnNew(ct, structName, fullStructName)

	// func Put()
	genCacheFnPut(ct, structName, fullStructName)

	// func Get()
	genCacheFnGet(ct, structName, fullStructName)

	// func Size()
	genCacheFnSize(ct, structName, fullStructName, fieldTools)

	// func Set()
	genCacheFnSet(ct, structName, fullStructName, fieldTools)

	// func ShallowCopy()
	genCacheFnShallowCopy(ct, structName, fullStructName, fieldTools)

	// func DeepCopy()
	genCacheFnDeepCopy(ct, structName, fullStructName, fieldTools)
}

type fieldOptions struct {
	Skip bool
}

func parseFieldOptions(field reflect.StructField) fieldOptions {
	skip := false
	// opts from tags
	for _, opt := range strings.Split(field.Tag.Get("heyicache"), ",") {
		switch opt {
		case "skip":
			skip = true
		}
	}

	return fieldOptions{
		Skip: skip,
	}
}

func getFieldTools(t reflect.Type) []*FieldTool {
	fieldTools := make([]*FieldTool, 0, t.NumField())
	// fields
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		opts := parseFieldOptions(field)
		fieldTools = append(fieldTools, &FieldTool{
			Name:       field.Name,
			TypeName:   field.Type.Name(),
			Type:       field.Type,
			IsExported: field.IsExported(),
			IsSkip:     opts.Skip || strings.HasPrefix(field.Name, "XXX_"),
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
		ct.Println(structName + "Empty = &" + fullStructName + "{}")
		ct.Println("// object pool")
		ct.Println(structName + "Pool = sync.Pool{")
		{
			ct.In()
			ct.Println("New: func() interface{} {")
			ct.In()
			ct.Println("return &" + fullStructName + "{}")
			ct.Out()
			ct.Println("},")
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

// Projection never reads excluded fields. Returning a value also makes the
// generated assignment safe when source and destination are the same object.
func genCacheFnProject(ct *CodeTool, structName, fullStructName string, fields []*FieldTool) {
	ct.Println("func heyicacheProject" + structName + "(src *" + fullStructName + ") " + fullStructName + " {")
	ct.In()
	ct.Println("var dst " + fullStructName)
	ct.ProjectFields(fields)
	ct.Println("return dst")
	ct.Out()
	ct.Println("}")
	ct.Println("")
}

func (ct *CodeTool) ProjectFields(fields []*FieldTool) {
	for _, field := range fields {
		field.Check()
		if field.omitted() {
			continue
		}
		switch field.Category {
		case FieldTypeStruct:
			ct.Println("dst." + field.Name + " = heyicacheProject" + field.TypeName + "(&src." + field.Name + ")")
		case FieldTypeArray:
			ct.ArrayFields(field, ct.ProjectFields)
		default:
			ct.Println("dst." + field.Name + " = src." + field.Name)
		}
	}
}

func (ct *CodeTool) ArrayFields(field *FieldTool, generate func([]*FieldTool)) {
	index := fmt.Sprintf("idx%d", ct.loopID)
	ct.loopID++
	ct.Println("for " + index + " := range src." + field.Name + " {")
	ct.In()
	generate([]*FieldTool{{Name: field.Name + "[" + index + "]", Type: field.Type.Elem(), IsExported: true}})
	ct.Out()
	ct.Println("}")
}

func arrayNeedsTraversal(t reflect.Type) bool {
	for t.Kind() == reflect.Array {
		t = t.Elem()
	}
	field := &FieldTool{Type: t, IsExported: true}
	field.Check()
	return !field.omitted() && field.Category != FieldTypeUnknown
}

func genCacheFnNew(ct *CodeTool, structName, fullStructName string) {
	ct.Println("func (ifc *" + ct.getFnIfc(structName) + ") New(usePool bool) interface{} {")
	ct.In()
	ct.Println("if usePool {")
	{
		ct.In()
		ct.Println("obj := " + structName + "Pool.Get().(*" + fullStructName + ")")
		ct.Println("*obj = *" + structName + "Empty")
		ct.Println("return obj")
		ct.Out()
	}
	ct.Println("} else {")
	{
		ct.In()
		ct.Println("return new(" + fullStructName + ")")
		ct.Out()
	}
	ct.Println("}")
	ct.Out()
	ct.Println("}")
	ct.Println("")
}

func genCacheFnPut(ct *CodeTool, structName, fullStructName string) {
	ct.Println("func (ifc *" + ct.getFnIfc(structName) + ") Put(value interface{}) {")
	ct.In()
	ct.Println("if value == nil {")
	{
		ct.In()
		ct.Println("return")
		ct.Out()
	}
	ct.Println("}")
	ct.Println("")
	ct.Println("obj, ok := value.(*" + fullStructName + ")")
	ct.Println("if !ok || obj == nil {")
	{
		ct.In()
		ct.Println("return")
		ct.Out()
	}
	ct.Println("}")
	ct.Println("")
	ct.Println(structName + "Pool.Put(obj)")
	ct.Out()
	ct.Println("}")
	ct.Println("")
}

func genCacheFnGet(ct *CodeTool, structName, fullStructName string) {
	ct.Println("func (ifc *" + ct.getFnIfc(structName) + ") Get(bs []byte) interface{} {")
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
	ct.Println("func (ifc *" + ct.getFnIfc(structName) + ") Size(value interface{}, isStructPtr bool) int32 {")
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
	ct.Println("func (ifc *" + ct.getFnIfc(structName) + ") Set(value interface{}, bs []byte, isStructPtr bool) (interface{}, int32) {")
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
		ct.Println("dst = (*" + fullStructName + ")(unsafe.Pointer(&bs[0]))")
		ct.Out()
	}
	ct.Println("}")
	// Arena addresses are not necessarily aligned. Do not emit a typed whole-
	// struct assignment here: Go's bulk write barrier requires alignment. Copy
	// only the projected local value, never the original object's private state.
	ct.Println("public := heyicacheProject" + structName + "(src)")
	ct.Println("publicBytes := unsafe.Slice((*byte)(unsafe.Pointer(&public)), ifc.StructSize)")
	ct.Println("arenaBytes := unsafe.Slice((*byte)(unsafe.Pointer(dst)), ifc.StructSize)")
	ct.Println("copy(arenaBytes, publicBytes)")

	// foo.A, foo.B, etc.
	ct.SetStructFields(fieldTools)

	ct.Println("return dst, size")
	ct.Out()
	ct.Println("}")
	ct.Println("")
}

func genCacheFnShallowCopy(ct *CodeTool, structName, fullStructName string, fieldTools []*FieldTool) {
	ct.Println("func (ifc *" + ct.getFnIfc(structName) + ") ShallowCopy(srcValue, dstValue interface{}) {")
	ct.In()
	ct.Println("if srcValue == nil || dstValue == nil {")
	{
		ct.In()
		ct.Println("return")
		ct.Out()
	}
	ct.Println("}")
	ct.Println("")
	ct.Println("src, ok := srcValue.(*" + fullStructName + ")")
	ct.Println("if !ok || src == nil {")
	{
		ct.In()
		ct.Println("return")
		ct.Out()
	}
	ct.Println("}")
	ct.Println("")
	ct.Println("dst, ok := dstValue.(*" + fullStructName + ")")
	ct.Println("if !ok || dst == nil {")
	{
		ct.In()
		ct.Println("return")
		ct.Out()
	}
	ct.Println("}")
	ct.Println("")
	ct.Println("*dst = heyicacheProject" + structName + "(src)")
	ct.Out()
	ct.Println("}")
	ct.Println("")
}

func genCacheFnDeepCopy(ct *CodeTool, structName, fullStructName string, fieldTools []*FieldTool) {
	ct.Println("func (ifc *" + ct.getFnIfc(structName) + ") DeepCopy(srcValue, dstValue interface{}) {")
	ct.In()
	ct.Println("if srcValue == nil || dstValue == nil {")
	{
		ct.In()
		ct.Println("return")
		ct.Out()
	}
	ct.Println("}")
	ct.Println("")

	ct.Println("src, ok := srcValue.(*" + fullStructName + ")")
	ct.Println("if !ok || src == nil {")
	{
		ct.In()
		ct.Println("return")
		ct.Out()
	}
	ct.Println("}")
	ct.Println("")

	ct.Println("dst, ok := dstValue.(*" + fullStructName + ")")
	ct.Println("if !ok || dst == nil {")
	{
		ct.In()
		ct.Println("return")
		ct.Out()
	}
	ct.Println("}")
	ct.Println("")

	ct.Println("// Snapshot public fields before overwriting a possibly aliased destination.")
	ct.Println("source := heyicacheProject" + structName + "(src)")
	ct.Println("src = &source")
	ct.Println("*dst = source")
	ct.Println("")

	// foo.A, foo.B, etc.
	ct.DeepCopyStructFields(fieldTools)

	ct.Out()
	ct.Println("}")
	ct.Println("")
}

type CodeTool struct {
	count             int
	loopID            int
	header            strings.Builder
	content           strings.Builder
	filename          string
	callerPkg         string
	callerPkgName     string
	isMainPkgStruct   bool
	objPkg            string
	subPkgs           []string
	needImport        bool
	needStringsImport bool
}

// camelToSnake converts camelCase to snake_case
// Example: RecommendStore -> recommend_store
func camelToSnake(s string) string {
	if len(s) == 0 {
		return s
	}

	var result strings.Builder
	for i, r := range s {
		if i > 0 && r >= 'A' && r <= 'Z' {
			result.WriteByte('_')
		}
		result.WriteRune(r)
	}
	return strings.ToLower(result.String())
}

func NewCodeTool(name, callerPkg, callerPkgName string, isMainPkgStruct bool, objPkg string, subPkgs []string) *CodeTool {
	// Extract the last part of the package name
	pkgParts := strings.Split(objPkg, "/") // eg: github.com/yuadsl3010/heyicache/o2oalgo
	objPkgName := pkgParts[len(pkgParts)-1]

	// Generate filename: heyicache_fn_{pkgName}_{name}.go
	filename := fmt.Sprintf("heyicache_fn_%s_%s.go",
		strings.ToLower(objPkgName),
		camelToSnake(name))

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
	ct.header.WriteString("\t\"sync\"\n")
	ct.header.WriteString("\t\"unsafe\"\n")
	if ct.needStringsImport {
		ct.header.WriteString("\t\"strings\"\n")
	}
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

func (ct *CodeTool) localType(name string) string {
	return strings.ReplaceAll(name, ct.callerPkgName+".", "")
}

func fieldVariable(name string) string {
	return strings.NewReplacer("[", "_", "]", "", ".", "_").Replace(name)
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
	v := fieldVariable(name)
	ct.Println("p" + v + ", size" + v + " := " + ct.getFuncSetSlice() + "(src." + name + ", bs[size:], " + ct.getFuncStructSize(typeName) + ")")
	ct.Println("size += size" + v)
	ct.CheckSize()
	ct.Println("dst." + name + " = p" + v)
}

func (ct *CodeTool) SetSliceCustom(name, typeName, fullTypeName string) {
	// Preserve the source slice before assigning dst: Set(false) works in place.
	ct.Println("{")
	ct.In()
	ct.Println("items := src." + name)
	ct.Println("dst." + name + " = nil")
	ct.Println("if len(items) > 0 {")
	ct.In()
	ct.Println("dst." + name + " = unsafe.Slice((*" + fullTypeName + ")(unsafe.Pointer(&bs[size])), len(items))")
	ct.Println("for idx := range items {")
	ct.In()
	ct.Println("public := heyicacheProject" + typeName + "(&items[idx])")
	ct.Println("width := int(unsafe.Sizeof(public))")
	ct.Println("publicBytes := unsafe.Slice((*byte)(unsafe.Pointer(&public)), width)")
	ct.Println("copy(bs[int(size)+idx*width:], publicBytes)")
	ct.Out()
	ct.Println("}")
	ct.Println("size += int32(len(items)) * int32(unsafe.Sizeof(items[0]))")
	ct.Out()
	ct.Println("}")
	ct.Out()
	ct.Println("}")
}

func (ct *CodeTool) setStruct(name, typeName, item string) {
	v := fieldVariable(name)
	ct.Println("_, size" + v + " := " + ct.getFnIfc_(typeName) + ".Set(" + item + ", bs[size:], false)")
	ct.Println("size += size" + v)
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
	v := fieldVariable(name)
	ct.Println("p" + v + ", size" + v + " := " + ct.getFnIfc_(typeName) + ".Set(" + item + ", bs[size:], true)")
	ct.Println("size += size" + v)
	ct.CheckSize()
	ct.Println("if p" + v + " != nil && size" + v + " > 0 {")
	ct.In()
	ct.Println("dst." + name + idx + " = p" + v + ".(" + ct.localType(fullName) + ")")
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
	v := fieldVariable(name)
	ct.Println("p" + v + ", size" + v + " := " + ct.getFuncSetString() + "(" + item + ", bs[size:])")
	ct.Println("size += size" + v)
	ct.CheckSize()
	ct.Println("dst." + name + idx + " = p" + v)
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

func (ct *CodeTool) DeepCopyStruct(name, typeName string) {
	ct.Println(ct.getFnIfc_(typeName) + ".DeepCopy(&src." + name + ", &dst." + name + ")")
}

func (ct *CodeTool) DeepCopyStructPtr(name, typeName, fullName string) {
	ct.Println("if src." + name + " != nil {")
	ct.In()
	ct.Println("dst." + name + " = " + ct.getFnIfc_(typeName) + ".New(false).(" + fullName + ")")
	ct.Println(ct.getFnIfc_(typeName) + ".DeepCopy(src." + name + ", dst." + name + ")")
	ct.Out()
	ct.Println("}")
}

func (ct *CodeTool) DeepCopyString(name string) {
	ct.needStringsImport = true
	ct.Println("dst." + name + " = strings.Clone(src." + name + ")")
}

func (ct *CodeTool) DeepCopySlice(name, typeName string) {
	ct.Println("if src." + name + " != nil {")
	ct.In()
	ct.Println("dst." + name + " = make([]" + typeName + ", len(src." + name + "))")
	ct.Println("copy(dst." + name + ", src." + name + ")")
	ct.Out()
	ct.Println("}")
}

func (ct *CodeTool) DeepCopySliceStruct(name, typeName, fullTypeName string) {
	ct.Println("if src." + name + " != nil {")
	ct.In()
	ct.Println("dst." + name + " = make([]" + ct.localType(fullTypeName) + ", len(src." + name + "))")
	ct.Println("for idx := range src." + name + " {")
	ct.In()
	ct.Println(ct.getFnIfc_(typeName) + ".DeepCopy(&src." + name + "[idx], &dst." + name + "[idx])")
	ct.Out()
	ct.Println("}")
	ct.Out()
	ct.Println("}")
}

func (ct *CodeTool) DeepCopySliceStructPtr(name, typeName, fullName string) {
	ct.Println("if src." + name + " != nil {")
	ct.In()
	ct.Println("dst." + name + " = make([]" + fullName + ", len(src." + name + "))")
	ct.Println("for idx, item := range src." + name + " {")
	ct.In()
	ct.Println("if item != nil {")
	ct.In()
	ct.Println("dst." + name + "[idx] = " + ct.getFnIfc_(typeName) + ".New(false).(" + fullName + ")")
	ct.Println(ct.getFnIfc_(typeName) + ".DeepCopy(item, dst." + name + "[idx])")
	ct.Out()
	ct.Println("}")
	ct.Out()
	ct.Println("}")
	ct.Out()
	ct.Println("}")
}

func (ct *CodeTool) DeepCopySliceString(name string) {
	ct.needStringsImport = true
	ct.Println(fmt.Sprintf("if src.%s != nil {", name))
	ct.In()
	ct.Println(fmt.Sprintf("dst.%s = make([]string, len(src.%s))", name, name))
	ct.Println(fmt.Sprintf("for idx, item := range src.%s {", name))
	ct.In()
	ct.Println(fmt.Sprintf("dst.%s[idx] = strings.Clone(item)", name))
	ct.Out()
	ct.Println("}")
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
		if field.omitted() {
			ct.Println("// omitted field remains zero: " + field.Name)
			continue
		}

		switch field.Category {
		case FieldTypeArray:
			if arrayNeedsTraversal(field.Type) {
				ct.ArrayFields(field, ct.SetStructFields)
			}
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
			ct.SetSliceCustom(field.Name, field.TypeName, ct.localType(field.FullTypeName))
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

func (ct *CodeTool) DeepCopyStructFields(fieldTools []*FieldTool) {
	for _, field := range fieldTools {
		ct.Println(field.Check())
		if field.omitted() {
			ct.Println("// omitted field remains zero: " + field.Name)
			continue
		}

		switch field.Category {
		case FieldTypeArray:
			if arrayNeedsTraversal(field.Type) {
				ct.ArrayFields(field, ct.DeepCopyStructFields)
			}
		case FieldTypeStruct: // foo Foo
			ct.Println("// struct: foo Foo")
			ct.DeepCopyStruct(field.Name, field.TypeName)
		case FieldTypeStructPtr: // foo *Foo
			ct.Println("// struct ptr: foo *Foo")
			ct.DeepCopyStructPtr(field.Name, field.TypeName, ct.localType(field.FullTypeName))
		case FieldTypeString: // foo string
			ct.Println("// string: foo string")
			ct.DeepCopyString(field.Name)
		case FieldTypeSliceStruct: // foo []Foo
			ct.Println("// slice struct: foo []Foo")
			ct.DeepCopySliceStruct(field.Name, field.TypeName, field.FullTypeName)
		case FieldTypeSliceStructPtr: // foo []*Foo
			ct.Println("// slice struct ptr: foo []*Foo")
			ct.DeepCopySliceStructPtr(field.Name, field.TypeName, ct.localType(field.FullTypeName))
		case FieldTypeSliceString: // foo []string
			ct.Println("// slice string: foo []string")
			ct.DeepCopySliceString(field.Name)
		case FieldTypeSlice: // foo []int, []byte, etc.
			ct.Println("// slice: foo []int, []byte, etc.")
			ct.DeepCopySlice(field.Name, field.TypeName)
		}
	}
	ct.Println("")
}

func (ct *CodeTool) SizeStructFields(fieldTools []*FieldTool) {
	for _, field := range fieldTools {
		ct.Println(field.Check())
		if field.omitted() {
			ct.Println("// skip field: " + field.Name)
			continue
		}
		switch field.Category {
		case FieldTypeArray:
			if arrayNeedsTraversal(field.Type) {
				ct.ArrayFields(field, ct.SizeStructFields)
			}
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
	FieldTypeArray          FieldType = 10
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

func (ft *FieldTool) omitted() bool {
	return !ft.IsExported || ft.IsSkip || ft.Category == FieldTypeNotStructPtr || ft.Category == FieldTypeMap
}

func (ft *FieldTool) Check() string {
	status := "success"
	if !ft.IsExported || ft.IsSkip {
		return fmt.Sprintf("// %s: %somitted; kept at its Go zero value", ft.Name, warn)
	}

	t := ft.Type
	switch t.Kind() {
	case reflect.Array:
		ft.Category = FieldTypeArray
	case reflect.Slice:
		switch t.Elem().Kind() {
		case reflect.Ptr:
			if t.Elem().Elem().Kind() != reflect.Struct {
				ft.Category = FieldTypeNotStructPtr
				break
			}
			name := t.Elem().String() // *heyicache.TestFoo
			names := strings.Split(name, ".")
			ft.TypeName = names[len(names)-1] // TestFoo
			ft.FullTypeName = getFullTypeName(name)
			ft.Category = FieldTypeSliceStructPtr
		case reflect.Struct:
			name := t.Elem().String() // heyicache.TestFoo
			names := strings.Split(name, ".")
			ft.TypeName = names[len(names)-1] // TestFoo
			ft.FullTypeName = getFullTypeName(name)
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
	case reflect.Interface, reflect.Func, reflect.Chan, reflect.UnsafePointer:
		ft.Category = FieldTypeNotStructPtr
	}

	switch ft.Category {
	case FieldTypeNotStructPtr:
		status = fmt.Sprintf("%sunsupported reference type; kept at its Go zero value", warn)
	case FieldTypeMap:
		status = fmt.Sprintf("%sskip and set nil! map type not supported cause it can't be stored by value, you must use custom serlization to store it if you really want map", warn)
	}

	return fmt.Sprintf("// %v: %v", ft.Name, status)
}

func validateCacheType(t reflect.Type, callerPkg string) {
	if t.Name() == "" || (t.PkgPath() != callerPkg && !token.IsExported(t.Name())) {
		panic(fmt.Sprintf("heyicache: cannot generate a codec for inaccessible or anonymous struct %s", t))
	}
	// The existing generator identifies codecs by the named struct type. All
	// reachable structs have already been collected and are validated separately.
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
	if t.Name() == "" {
		panic(fmt.Sprintf("heyicache: anonymous public struct %s is not supported; use a named type", t))
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
		opts := parseFieldOptions(field)
		if !field.IsExported() || opts.Skip || strings.HasPrefix(field.Name, "XXX_") {
			continue
		}

		fieldType := field.Type
		for fieldType.Kind() == reflect.Array {
			fieldType = fieldType.Elem()
		}
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
