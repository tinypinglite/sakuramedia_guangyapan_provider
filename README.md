# SakuraMedia 光鸭云盘 Provider

把光鸭云盘（guangyapan.com）接入 SakuraMedia 的媒体库，支持文件浏览、影片导入、播放、缩略图、片段和云添加下载。

## 安装

在 SakuraMedia 的「系统设置 → 插件」点击「安装插件」，上传本插件的 `.zip` 包，启用后重启 SakuraMedia 容器。

## 准备 refresh_token

插件用一个 `refresh_token` 登录你的光鸭账号，只需要获取一次。

1. 浏览器登录 <https://www.guangyapan.com>。
2. 按 F12 打开控制台。Chrome / Edge 第一次在这里粘贴时会被拦截，按提示手动输入 `allow pasting` 回车，再重新粘贴即可。
3. 粘贴下面这一行并回车，`refresh_token` 会复制到剪贴板：

```js
(()=>{for(const v of Object.values(localStorage)){try{const o=JSON.parse(v);if(o&&o.refresh_token){copy(o.refresh_token);console.log(o.refresh_token);return}}catch(e){}}console.log("未找到 refresh_token，请确认已登录")})()
```

> `refresh_token` 等同于账号权限，不要发到 issue、日志或截图里。失效后（比如在光鸭里登出了所有设备）重新获取一次即可。

## 创建媒体库

在「系统设置 → 媒体库」点击「新增媒体库」，将「存储 Provider」设为「光鸭云盘」，再填写：

- 名称：媒体库的显示名称。
- 光鸭 refresh_token：上一步复制的值。
- 光鸭媒体目录：导入影片的目标目录，填光鸭云盘里的绝对路径，需要先自己在光鸭中创建。
- 光鸭下载目录：云添加的保存目录，同样需要先创建；不用云添加可以留空。

保存时会校验 token 是否有效、目录是否存在。

## 使用

- 影片可以从光鸭账号的任意目录导入，导入后在媒体目录下按 `jav/<番号>/` 结构存放。
- 播放默认走 302 直链，也可以由后端代理；缩略图、片段、时长和编码由插件现场抽帧计算。
- 在「系统设置 → 下载器」新建下载器并选择这个光鸭库后，可以在下载任务里提交磁力链或种子，由光鸭直接拉取。
- 后台任务页提供「清理空目录」和「清空回收站」两个手动任务。

## 注意
需要开通光鸭云盘会员后使用.

## License

本项目采用 [PolyForm Noncommercial License 1.0.0](./LICENSE)，仅允许非商业用途。

Copyright (C) 2026 tinypinglite
