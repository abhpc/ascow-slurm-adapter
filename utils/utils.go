package utils

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"time"

	"os/exec"
	"os/user"

	pb "scow-slurm-adapter/gen/go"
	"strings"
	"syscall"

	"gopkg.in/yaml.v3"
)

type MySQLConfig struct {
	Host           string `yaml:"host"`
	Port           int    `yaml:"port"`
	User           string `yaml:"user"`
	DBName         string `yaml:"dbname"`
	Password       string `yaml:"password"`
	ClusterName    string `yaml:"clustername"`
	DatabaseEncode string `yaml:"databaseencode"`
}

type Service struct {
	Port int `yaml:"port"`
}

type Slurm struct {
	DefaultQOS string `yaml:"defaultqos"`
	Slurmpath  string `yaml:"slurmpath,omitempty"`
}

type Modulepath struct {
	Path string `yaml:"path"`
}

type Config struct {
	MySQLConfig MySQLConfig `yaml:"mysql"`
	Service     Service     `yaml:"service"`
	Slurm       Slurm       `yaml:"slurm"`
	Modulepath  Modulepath  `yaml:"modulepath"`
}

var (
	DefaultConfigPath string = "config/config.yaml"
)

// 解析配置文件
func ParseConfig(configFilePath string) *Config {
	confFile, err := ioutil.ReadFile(configFilePath)
	if err != nil {
		log.Fatal(err)
	}
	config := &Config{}

	err = yaml.Unmarshal(confFile, config)
	if err != nil {
		log.Fatal(err)
	}
	return config
}

// 带返回码的shell命令执行函数
func ExecuteShellCommand(command string) int {
	var (
		res int
	)
	cmd := exec.Command("bash", "-c", command)
	stdout, _ := cmd.StdoutPipe()
	defer stdout.Close()
	if err := cmd.Start(); err != nil {
		panic(err)
	}
	if err := cmd.Wait(); err != nil {
		if ex, ok := err.(*exec.ExitError); ok {
			res = ex.Sys().(syscall.WaitStatus).ExitStatus()
		}
	}
	return res
}

// 简单执行shell命令函数
func RunCommand(command string) (string, error) {
	// cmd := exec.Command("bash", "-c", command)
	// output, err := cmd.CombinedOutput()
	// if err != nil {
	// 	return "", err
	// }
	// return strings.TrimSpace(string(output)), nil
	cmd := exec.Command("bash", "-c", command)

	// 创建一个 bytes.Buffer 用于捕获输出
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output

	// 执行命令
	err := cmd.Run()

	if err != nil {
		return output.String(), err
	}

	return strings.TrimSpace(output.String()), nil
}

// 数据库配置信息
func DatabaseConfig() string {
	config := ParseConfig(DefaultConfigPath)
	host := config.MySQLConfig.Host
	userName := config.MySQLConfig.User
	passWord := config.MySQLConfig.Password
	dbName := config.MySQLConfig.DBName
	port := config.MySQLConfig.Port
	databaseencode := config.MySQLConfig.DatabaseEncode

	// dbConfig := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s", userName, passWord, host, port, dbName, "latin1")
	dbConfig := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s", userName, passWord, host, port, dbName, databaseencode)
	return dbConfig
}

// 获取全系统计算分区信息
func GetPatitionInfo() ([]string, error) {
	shellCmd := "scontrol show partition| grep PartitionName=| awk -F'=' '{print $2}'| tr '\n' ','"
	cmd := exec.Command("bash", "-c", shellCmd)

	// 创建一个 bytes.Buffer 用于捕获输出
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output

	// 执行命令
	err := cmd.Run()
	if err != nil {
		return nil, err
	}
	resOutput := strings.Split(strings.TrimSpace(output.String()), ",")
	resOutput = resOutput[:len(resOutput)-1]
	return resOutput, nil
}

func DeleteSlice(data []string, word string) []string {
	tmp := make([]string, 0, len(data))
	for _, v := range data {
		if v != word {
			tmp = append(tmp, v)
		}
	}
	return tmp
}

// 作业状态码转换
func ChangeState(stateInit int) string {
	var (
		stateString string
	)
	switch stateInit {
	case 0:
		stateString = "PENDING"
	case 1:
		stateString = "RUNNING"
	case 2:
		stateString = "SUSPEND"
	case 3:
		stateString = "COMPLETED"
	case 4:
		stateString = "CANCELED"
	case 5:
		stateString = "FAILED"
	case 6:
		stateString = "TIMEOUT"
	case 7:
		stateString = "NODE_FAIL"
	default:
		stateString = "COMPLETED"
	}
	return stateString
}

// 作业状态码转换
func GetStateId(stateString string) int {
	var (
		state int
	)
	switch stateString {
	case "PENDING":
		state = 0
	case "RUNNING":
		state = 1
	case "SUSPEND":
		state = 2
	case "COMPLETED":
		state = 3
	case "CANCELED":
		state = 4
	case "FAILED":
		state = 5
	case "TIMEOUT":
		state = 6
	case "NODE_FAIL":
		state = 7
	default:
		state = 3
	}
	return state
}

func GetTimeLimit(timeLimit string) int64 {
	var timeLimitMinutes int64
	if strings.Contains(timeLimit, "-") {
		timeLimitMinutesList := strings.Split(timeLimit, "-")
		day, _ := strconv.Atoi(timeLimitMinutesList[0])
		timeLimitMinutesListNew := strings.Split(timeLimitMinutesList[1], ":")
		hours, _ := strconv.Atoi(timeLimitMinutesListNew[0])
		minutes, _ := strconv.Atoi(timeLimitMinutesListNew[1])
		seconds, _ := strconv.Atoi(timeLimitMinutesListNew[2])
		return int64(seconds)*0 + int64(minutes)*1 + int64(hours)*60 + int64(day)*24*60
	} else {
		// 没有timeLimitMinutes超过一天的作业
		timeLimitMinutesList := strings.Split(timeLimit, ":")
		if len(timeLimitMinutesList) == 2 {
			minutes, _ := strconv.Atoi(timeLimitMinutesList[0])
			seconds, _ := strconv.Atoi(timeLimitMinutesList[1])
			timeLimitMinutes = int64(seconds)*0 + int64(minutes)*1
		} else {
			hours, _ := strconv.Atoi(timeLimitMinutesList[0])
			minutes, _ := strconv.Atoi(timeLimitMinutesList[1])
			seconds, _ := strconv.Atoi(timeLimitMinutesList[2])
			timeLimitMinutes = int64(seconds)*0 + int64(minutes)*1 + int64(hours)*60
		}
		return timeLimitMinutes
	}
}

func GetGpuAllocsFromGpuId(matchCmd string, gpuId int, tresAlloc string) int32 {
	var (
		gpusAlloc int32
	)
	res := ExecuteShellCommand(matchCmd)
	if res == 0 {
		resAllocList := strings.Split(tresAlloc, ",")
		for _, v := range resAllocList {
			vList := strings.Split(v, "=")
			id := vList[0]
			number := vList[1]
			idInt, _ := strconv.Atoi(id)
			numberInt, _ := strconv.Atoi(number)
			if idInt == gpuId {
				gpusAlloc = int32(numberInt)
				return gpusAlloc
			}
		}
	}
	return 0
}

func GetGpuAllocsFromGpuIdList(tresAlloc string, gpuId []int) int32 {
	var (
		gpusAlloc int32
	)
	resAllocList := strings.Split(tresAlloc, ",")
	for _, idValue := range gpuId {
		for _, resAlloc := range resAllocList {
			resAllocKey := strings.Split(resAlloc, "=")
			id := resAllocKey[0]
			idInt, _ := strconv.Atoi(id)
			if idInt == idValue {
				number := resAllocKey[1]
				numberInt, _ := strconv.Atoi(number)
				gpusAlloc = int32(numberInt)
				return gpusAlloc
			}
		}
	}
	return gpusAlloc
}

// 通过作业表中的tres信息解析获取资源信息
func GetResInfoNumFromTresInfo(tresInfo string, resId int) int {
	var (
		resInfoNum int
	)
	resAllocList := strings.Split(tresInfo, ",")
	for _, resInfo := range resAllocList {
		resInfoKey := strings.Split(resInfo, "=")
		id := resInfoKey[0]
		idInt, _ := strconv.Atoi(id)
		if idInt == resId {
			tresNum := resInfoKey[1]
			tresNumInt, _ := strconv.Atoi(tresNum)
			resInfoNum = tresNumInt
			return resInfoNum
		}
	}
	return resInfoNum
}

// 根据用户名获取系统中对应的 uid 和 gid。
// 由于底层调用 user.Lookup() 会经过 NSS（如 LDAP/NIS），当用户不存在于
// 认证系统或认证服务响应慢时可能无限阻塞，因此将查询放入独立 goroutine，
// 并通过 select 设置 5 秒超时：超时后返回 error，调用方应跳过该用户。
func GetUserUidGid(username string) (int, int, error) {
	type result struct {
		uid int
		gid int
		err error
	}
	ch := make(chan result, 1)
	go func() {
		u, err := user.Lookup(username)
		if err != nil {
			ch <- result{-1, -1, err}
			return
		}
		uidInt, _ := strconv.Atoi(u.Uid)
		gidInt, _ := strconv.Atoi(u.Gid)
		ch <- result{uidInt, gidInt, nil}
	}()
	select {
	case r := <-ch:
		return r.uid, r.gid, r.err
	case <-time.After(5 * time.Second):
		// 5 秒内未收到结果，判定为超时（LDAP 等认证服务无响应）
		return -1, -1, fmt.Errorf("timeout looking up OS user %s", username)
	}
}

// 根据 uid 反查操作系统用户名。同 GetUserUidGid，使用 goroutine + 超时
// 防止 LDAP 查询慢导致调用方挂起，超时阈值同为 5 秒。
func GetUserNameByUid(uid int) (string, error) {
	type result struct {
		name string
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		u, err := user.LookupId(strconv.Itoa(uid))
		if err != nil {
			ch <- result{"", err}
			return
		}
		ch <- result{u.Username, nil}
	}()
	select {
	case r := <-ch:
		return r.name, r.err
	case <-time.After(5 * time.Second):
		// 5 秒内未收到结果，判定为超时
		return "", fmt.Errorf("timeout looking up OS user by uid %d", uid)
	}
}

// 判断字符串中是否包含大写字母
func CheckAccountOrUserStrings(s string) bool {

	pattern := "^[a-z0-9_]+$"
	// 编译正则表达式
	reg := regexp.MustCompile(pattern)
	// 使用正则表达式判断字符串是否符合模式
	if reg.MatchString(s) {
		return true
		// fmt.Println("字符串符合要求")
	} else {
		// fmt.Println("字符串不符合要求")
		return false
	}

	// for _, char := range s {
	// 	if unicode.IsUpper(char) {
	// 		return true
	// 	}
	// }
	// return false
}

// 本地提交作业函数
func LocalSubmitJob(scriptString string, username string) (string, error) {
	// 提交作业命令行
	// cmdLine := fmt.Sprintf("su - %s -c '/usr/bin/sbatch'", username)
	config := ParseConfig(DefaultConfigPath)
	slurmpath := config.Slurm.Slurmpath
	if slurmpath == "" {
		// 如果未定义，则将其设置为默认值 "/usr"
		slurmpath = "/usr"
	}
	cmdLine := fmt.Sprintf("su - %s -c '%s/bin/sbatch'", username, slurmpath)
	// cmdLine := fmt.Sprintf("su - %s -c '/usr/bin/sbatch'", username)
	cmd := exec.Command("bash", "-c", cmdLine)

	// 创建一个 bytes.Buffer 用于捕获输出
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output

	// 将脚本作为命令的输入
	cmd.Stdin = bytes.NewBufferString(scriptString)

	// 执行命令
	err := cmd.Run()
	if err != nil {
		return output.String(), err
	}

	return output.String(), nil
}

func ExtractValue(input, key string) string {
	// 构建匹配键值对的正则表达式
	pattern := fmt.Sprintf("%s=([^\\s]+)", key)
	re := regexp.MustCompile(pattern)

	// 查找第一个匹配项
	match := re.FindStringSubmatch(input)
	if len(match) >= 2 {
		return match[1]
	}
	return ""
}

func LocalFileSubmitJob(filePath string, username string) (string, error) {
	config := ParseConfig(DefaultConfigPath)
	slurmpath := config.Slurm.Slurmpath
	if slurmpath == "" {
		// 如果未定义，则将其设置为默认值 "/usr"
		slurmpath = "/usr"
	}
	cmdLine := fmt.Sprintf("su - %s -c '%s/bin/sbatch %s'", username, slurmpath, filePath)
	// cmdLine := fmt.Sprintf("su - %s -c '/usr/bin/sbatch %s'", username, filePath)
	cmd := exec.Command("bash", "-c", cmdLine)
	// 创建一个 bytes.Buffer 用于捕获输出
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output

	// 将脚本作为命令的输入

	// 执行命令
	err := cmd.Run()
	if err != nil {
		return output.String(), err
	}

	return output.String(), nil
}

func GetUserHomedir(username string) (string, error) {
	// 获取指定用户名的用户信息
	u, err := user.Lookup(username)
	if err != nil {
		return "", err
	}

	// 获取家目录
	homeDir := u.HomeDir
	return homeDir, nil
}

// 取消作业函数
func LocalCancelJob(username string, jobId int) (string, error) {
	config := ParseConfig(DefaultConfigPath)
	slurmpath := config.Slurm.Slurmpath
	if slurmpath == "" {
		// 如果未定义，则将其设置为默认值 "/usr"
		slurmpath = "/usr"
	}
	cmdLine := fmt.Sprintf("su - %s -c '%s/bin/scancel %d'", username, slurmpath, jobId)
	// cmdLine := fmt.Sprintf("su - %s -c 'scancel %d'", username, jobId)
	cmd := exec.Command("bash", "-c", cmdLine)
	// 创建一个 bytes.Buffer 用于捕获输出
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output

	// 执行命令
	err := cmd.Run()
	if err != nil {
		return output.String(), err
	}

	return output.String(), nil
}

// 获取map信息
func GetMapInfo(pendingString string) map[int]string {
	m := make(map[int]string)

	pairs := strings.Split(pendingString, ",")
	for _, pair := range pairs {
		kv := strings.Split(pair, " ")
		if len(kv) != 2 {
			continue
		}
		key, err := strconv.Atoi(kv[0])
		if err != nil {
			continue
		}
		value := strings.Trim(kv[1], "()")
		m[key] = value
	}
	return m
}

// 获取map信息
func GetPendingMapInfo(pendingString string) map[int]string {
	m := make(map[int]string)

	pairs := strings.Split(pendingString, ";")
	for _, pair := range pairs {
		kv := strings.Split(pair, "=")
		if len(kv) != 2 {
			continue
		}
		key, err := strconv.Atoi(kv[0])
		if err != nil {
			continue
		}
		value := strings.Trim(kv[1], "()")
		m[key] = value
	}
	return m
}

// 判断arr2 是否为arr1的子集
func IsSubSet(arr1, arr2 []string) bool {
	// 创建一个map，用于记录arr1中的元素
	m := make(map[string]bool)
	// 将arr1中的元素添加到map中
	for _, num := range arr1 {
		m[num] = true
	}
	// 遍历arr2中的元素，判断是否都在map中
	for _, num := range arr2 {
		if !m[num] {
			return false
		}
	}
	return true
}

func GetRunningElapsedSeconds(timeString string) int64 {
	var elapsedSeconds int64
	if strings.Contains(timeString, "-") {
		ElapsedSecondsList := strings.Split(timeString, "-")
		day, _ := strconv.Atoi(ElapsedSecondsList[0])
		ElapsedSecondsListNew := strings.Split(ElapsedSecondsList[1], ":")
		hours, _ := strconv.Atoi(ElapsedSecondsListNew[0])
		minutes, _ := strconv.Atoi(ElapsedSecondsListNew[1])
		seconds, _ := strconv.Atoi(ElapsedSecondsListNew[2])
		return int64(seconds) + int64(minutes)*60 + int64(hours)*3600 + int64(day)*24*3600
	} else {
		// 没有超过一天的作业
		ElapsedSecondsList := strings.Split(timeString, ":")
		if len(ElapsedSecondsList) == 2 {
			minutes, _ := strconv.Atoi(ElapsedSecondsList[0])
			seconds, _ := strconv.Atoi(ElapsedSecondsList[1])
			elapsedSeconds = int64(seconds) + int64(minutes)*60
		} else {
			hours, _ := strconv.Atoi(ElapsedSecondsList[0])
			minutes, _ := strconv.Atoi(ElapsedSecondsList[1])
			seconds, _ := strconv.Atoi(ElapsedSecondsList[2])
			elapsedSeconds = int64(seconds) + int64(minutes)*60 + int64(hours)*3600
		}
		return elapsedSeconds
	}
}

func sortByKey(list []*pb.JobInfo, fieldName string, sortOrder string) bool {
	if sortOrder == "ASC" {
		sort.Slice(list, func(i, j int) bool {
			fieldValueI := reflect.ValueOf(list[i]).Elem().FieldByName(fieldName)
			fieldValueJ := reflect.ValueOf(list[j]).Elem().FieldByName(fieldName)
			switch fieldValueI.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				return fieldValueI.Int() < fieldValueJ.Int()
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				return fieldValueI.Uint() > fieldValueJ.Uint()
			case reflect.Float32, reflect.Float64:
				return fieldValueI.Float() < fieldValueJ.Float()
			case reflect.String:
				return fieldValueI.String() < fieldValueJ.String()
			default:
				return false
			}
		})
	} else if sortOrder == "DESC" {
		sort.Slice(list, func(i, j int) bool {
			fieldValueI := reflect.ValueOf(list[i]).Elem().FieldByName(fieldName)
			fieldValueJ := reflect.ValueOf(list[j]).Elem().FieldByName(fieldName)
			switch fieldValueI.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				return fieldValueI.Int() > fieldValueJ.Int()
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				return fieldValueI.Uint() > fieldValueJ.Uint()
			case reflect.Float32, reflect.Float64:
				return fieldValueI.Float() > fieldValueJ.Float()
			case reflect.String:
				return fieldValueI.String() > fieldValueJ.String()
			default:
				return false
			}
		})
	}
	return true
}

func SortJobInfo(sortKey string, sortOrder string, jobInfo []*pb.JobInfo) []*pb.JobInfo {
	sortByKey(jobInfo, sortKey, sortOrder)
	return jobInfo
}

func CheckSlurmStatus(result string) bool {
	subStr := "Unable to contact slurm controller"
	if strings.Contains(result, subStr) {
		return true
	} else {
		return false
	}
}

func GetSlurmPath() string {
	config := ParseConfig(DefaultConfigPath)
	slurmpath := config.Slurm.Slurmpath
	if slurmpath == "" {
		// 如果未定义，则将其设置为默认值 "/usr"
		slurmpath = "/usr"
	}
	return slurmpath
}

// GetSlurmConfPath 通过 scontrol show conf 获取 slurm.conf 的绝对路径。
// slurm.conf 的实际位置因集群安装路径而异，不能硬编码，需动态获取。
func GetSlurmConfPath() (string, error) {
	output, err := RunCommand("scontrol show conf | grep '^SLURM_CONF' | awk '{print $NF}'")
	if err != nil {
		return "", fmt.Errorf("failed to get SLURM_CONF path: %w", err)
	}
	return strings.TrimSpace(output), nil
}

// GetPartitionAllowAccountsFromConf 解析 slurm.conf 及其通过 include 指令
// 引入的所有子文件，返回"分区名 -> AllowAccounts 值"的映射表。
//
// 只收录显式设置了非 ALL 值的分区；不在返回 map 中的分区表示其
// AllowAccounts=ALL（允许所有账户）或未设置该字段，调用方应按动态逻辑处理。
func GetPartitionAllowAccountsFromConf() (map[string]string, error) {
	confPath, err := GetSlurmConfPath()
	if err != nil {
		return nil, err
	}
	if confPath == "" {
		return nil, fmt.Errorf("SLURM_CONF path is empty")
	}
	result := make(map[string]string)
	// visited 用于防止 include 循环引用导致死递归
	if err := parseConfFile(confPath, result, make(map[string]bool)); err != nil {
		return nil, err
	}
	return result, nil
}

// parseConfFile 递归读取一个 slurm.conf 格式的文件及其 include 文件，
// 将 PartitionName -> AllowAccounts 的映射写入 result。
//
// 解析规则：
//   - '#' 及其后的内容视为注释，在处理前截断
//   - 'include <路径>' 行递归解析指定文件（支持 glob 通配符）
//   - 以 PartitionName= 开头的行为分区定义，字段名大小写不敏感
//   - 分区名和账户名的值保留原始大小写（Slurm 对分区名区分大小写）
//   - AllowAccounts=ALL 或未设置的分区不写入 result
func parseConfFile(path string, result map[string]string, visited map[string]bool) error {
	// 防止循环 include
	if visited[path] {
		return nil
	}
	visited[path] = true

	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	dir := filepath.Dir(path)
	for _, rawLine := range strings.Split(string(data), "\n") {
		// 截断行内注释并去除首尾空白
		line := rawLine
		if idx := strings.Index(line, "#"); idx >= 0 {
			line = line[:idx]
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// 用小写版本做关键字匹配，保持大小写不敏感
		lower := strings.ToLower(line)

		// 处理 include 指令，递归解析引入的配置文件
		if strings.HasPrefix(lower, "include ") {
			parts := strings.Fields(line)
			if len(parts) < 2 {
				continue
			}
			inc := parts[1]
			// 相对路径以 slurm.conf 所在目录为基准
			if !filepath.IsAbs(inc) {
				inc = filepath.Join(dir, inc)
			}
			// 支持通配符（如 include /etc/slurm/partitions_*.conf）
			matches, _ := filepath.Glob(inc)
			for _, m := range matches {
				_ = parseConfFile(m, result, visited)
			}
			continue
		}

		// 只处理分区定义行（以 PartitionName= 开头）
		if !strings.HasPrefix(lower, "partitionname=") {
			continue
		}

		// 逐字段解析 key=value，提取 PartitionName 和 AllowAccounts
		var partName, allowAccounts string
		for _, field := range strings.Fields(line) {
			idx := strings.Index(field, "=")
			if idx < 0 {
				continue
			}
			k := strings.ToLower(field[:idx]) // 字段名不区分大小写
			v := field[idx+1:]                // 值保留原始大小写
			switch k {
			case "partitionname":
				partName = v
			case "allowaccounts":
				allowAccounts = v
			}
		}
		// 仅记录明确限制了账户列表的分区（非空且非 ALL）
		if partName != "" && allowAccounts != "" && !strings.EqualFold(allowAccounts, "ALL") {
			result[partName] = allowAccounts
		}
	}
	return nil
}
