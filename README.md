# DeerTavern

DeerTavern 是 [SillyTavern](https://github.com/SillyTavern/SillyTavern) 的一个 fork。SillyTavern 是面向高级用户的大语言模型前端。

## 相比于 SillyTavern 的修改

1. **后台生成与回答恢复：** 为 OpenAI 聊天补全、Text Generation WebUI、NovelAI 和 Kobold 增加了服务器端生成任务。只要服务器保持运行，即使刷新或关闭浏览器页面，或遇到短暂断网，生成仍会继续；服务器会保存输出，并在前端重新连接或重新打开聊天后恢复尚未交付的回答，同时避免重复交付已保存的回答。手动停止会取消对应任务；服务器重启会中断正在进行的生成，只能恢复已保存的部分输出。目前尚未覆盖 Horde 生成。

## 相关资源

- GitHub：<https://github.com/SillyTavern/SillyTavern>
- 文档：<https://docs.sillytavern.app/>
- Discord：<https://discord.gg/sillytavern>
- Reddit：<https://reddit.com/r/SillyTavernAI>

## 许可证

AGPL-3.0
