## 项目说明

1. 下载在保利威存储的视频文件：   
    a.读取Excel文件中的视频VID   
    b.调用保利威接口获取视频VID对应的下载URL  
    c.下载视频文件，文件名称为VID   
    d.将下载结果写入到EXCEL结果文件中   
2. 程序支持功能包括:   
    a.支持下载EXCEL中指定行数处理下载任务  
    b.支持配置参数，强制重新下载视频  
    c.支持配置参数，指定下载存储根路径   
    d.支持配置参数，指定下载工作线程数量 
    e.支持配置参数，指定线程池等待结束时间      

### 文档说明：
* 依赖包清单 *   
	1. 环境依赖包清单在项目根下doc目录下文件。
* 批量安装依赖包 *
	2. pip3.9 install -r requirements.txt

### 配置文件：
* 配置文件 *
    1. 配置文件在:/dlvideo/settings.py    

### 执行方式：
1. 命令行帮助 *
    a. python3.9 handle.py --help
    b. python3.9 handle.py -h
2. 执行命令 *
    a. python3.9 handle.py --file 'vid-20210214.xlsx' --sheet 'listing' --start 2 --end 4
    b. python3.9 handle.py -f 'vid-20210214.xlsx' -t 'listing' -s 2 -e 4
