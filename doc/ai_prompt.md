# heyicache 接入ai手册

## 1. codec 生成

### 1.1 go.mod修改
执行`go get github.com/yuadsl3010/heyicache`

### 1.1 struct结构体收集
创建`heyicache_codec.md`文件，内容如下
```markdown
cache Get/Set 需要处理的struct对象：
${human_input} // 按照"package" + space + "struct name"的方式组成。eg: "github.com/yuadsl3010/o2oalgo" Store
${human_input}
```
如果`heyicache_codec.md`文件中仍然有${human_input}，则提示用户手动对文件进行修改，修改完成后进行下一步

### 1.2 codec代码生成
在项目根目录创建`heyicache_codec`文件夹，如果已经存在则跳过

在`heyicache_codec`文件夹中创建`codec_test.go`文件，如果已经存在修改`TestFnGenerateTool`函数就好，内容如下

```golang
package heyicache_codec
import (
	"testing"

	"github.com/yuadsl3010/heyicache"
)

// you can use this tool to generate the function Set(), Get() and Size()
func TestFnGenerateTool(t *testing.T) {
	heyicache.GenCacheFn(${package.sturct_name}) // 根据`heyicache_codec.md`文件中需要处理的struct对象进行生成，注意不能为指针，直接使用struct结构体就可以。eg: heyicache.GenCacheFn(o2oalgo.Store{})
	heyicache.GenCacheFn(${package.sturct_name})
}
```
生成完成后执行这个test文件，`heyicache_codec`目录下出现新的heyicache_fn_文件说明成功，进行下一步

## 2. cache 创建
### 2.1 配置信息收集
创建`heyicache_config.md`文件，内容如下
```markdown
cache 1
Name: ${human_input} // 独一无二的名字。eg: "heyi"
MaxSize: ${human_input} // MB为单位，最小为32MB。eg: 2048 // 2GB
IsStorage: ${human_input} // 是否为存储模式，适用于无需expire数据的场景。eg: false
VersionStorage: ${human_input} // 如果是存储模式，则VersionStorage应该从1开始，每一次全量刷新存储，应该通过oldCache.NextVersion()获取下一个版本号。eg: 1
IsStorageUnlimited: ${human_input} // 如果是存储模式，是否要限定最大存储大小。eg: false

cache 2
...
```
如果`heyicache_config.md`文件中仍然有${human_input}，则提示用户手动对文件进行修改，修改完成后进行下一步

### 2.2 cache init
根据`heyicache_config.md`文件在合适的位置创建heyicache对象，例子如下
```golang
var Heyi *heyicache.Cache
func init() {
    // cache创建
    c, err := heyicache.NewCache(heyicache.Config{
        Name:               name,
        MaxSize:            maxSize,
        CustomTimer:        heyicache.NewCachedTimer(),
    })
    if err != nil {
        panic(err)
    }

    Heyi = c

    // 监控上报
    go func() {
        ticker := time.NewTicker(15 * time.Second)
        for range ticker.C {
            name := c.Name
            stat := c.GetAndResetStat()
            _ = name
            _ = stat
            // TODO
            // report(name, "ReportCount", float64(1))
            // report(name, "EvictionNum", float64(stat.EvictionNum))
            // report(name, "EvictionCount", float64(stat.EvictionCount))
            // report(name, "EvictionWaitCount", float64(stat.EvictionWaitCount))
            // report(name, "ExpireCount", float64(stat.ExpireCount))
            // report(name, "OverwriteCount", float64(stat.OverwriteCount))
            // report(name, "SkipWriteCount", float64(stat.SkipWriteCount))
            // report(name, "HitCount", float64(stat.HitCount))
            // report(name, "MissCount", float64(stat.MissCount))
            // report(name, "ReadCount", float64(stat.ReadCount))
            // report(name, "WriteCount", float64(stat.WriteCount))
            // report(name, "WriteErrCount", float64(stat.WriteErrCount))
            // report(name, "MemUsed", float64(stat.MemUsed))
            // report(name, "MemSize", float64(stat.MemSize))
            // report(name, "EntryCount", float64(stat.EntryCount))
            // report(name, "EntryCap", float64(stat.EntryCap))
        }
    }()
}
```
创建完成后进行下一步

## 3. cache 读写
### 3.1 Get 代码生成
在合适的位置创建Get函数
```golang
func GetCache(lease *heyicache.Lease, key string) (&${package.struct_name}, error) {
	obj, err := Heyi.Get(lease, []byte(key), heyicache_codec.HeyiCacheFn${struct_name}Ifc_)
	if err != nil {
		return nil, err
	}

	return obj.(*${package.struct_name})
```
调用Get函数时需要传入lease参数，该参数需要在ctx处初始化，lease尽可能的复用以提升性能
```golang
leaseCtx := heyicache.GetLeaseCtx(ctx)
lease := leaseCtx.GetLease(Heyi)
obj, err := GetCache(lease, "test")
...
```

### 3.2 Set 代码生成
在合适的位置创建Get函数
```golang
func SetCache(key string, obj *{package.struct_name}, expireSec int) error {
    if expireSec == 0 {
        return nil
    }

	return Heyi.Set([]byte(key), obj, heyicache_codec.HeyiCacheFn${struct_name}Ifc_, expireSec)
```

### 3.3 lease 释放
在整个cache创建的地方增加下列注释
```golang
// BE CAREFUL!!!
// highly recommend add lease New()/Done() code at the start/end of the ctx lifetime, eg:
// // init a new context with heyi cache lease
// ctx = heyicache.NewLeaseCtx(ctx)
// rps, err := handler(ctx, req)
// // if the framwork support middleware, you can also write a middleware for putting Done() after the rsp sent
// time.AfterFunc(5*time.Second, func() {
//     heyicache.GetLeaseCtx(ctx).Done()
// })
var Heyi *heyicache.Cache
```
完成后执行下一步

## 4. 编译
编译确认正确后执行下一步

## 5. 约束校验
从heyicache.Get()后的结构体，是不可以修改里面的成员指针的

成员指针包括string、slice、struct ptr等等

但是可以修改里面的成员值，例如uint64、int32、float64等等

所以需要检查代码中，是否有对成员指针进行修改的情况

如果有，需要提示用户配合进行相关修改

如果没有，则提示用户heyicache约束校验通过
