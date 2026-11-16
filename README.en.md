# DeerTavern

A fork of [SillyTavern](https://github.com/SillyTavern/SillyTavern), an LLM frontend for power users.

## Changes from SillyTavern

1. **Background generation and response recovery:** Added server-side generation jobs for OpenAI chat completions, Text Generation WebUI, NovelAI, and Kobold. As long as the server remains running, generation continues through browser refreshes, closed pages, and temporary network interruptions. The server saves output so the frontend can replay it after reconnecting or reopening the chat, while avoiding duplicate delivery if the answer was already saved. Manually stopping a generation cancels its job. A server restart interrupts active generation, so only saved partial output can be restored. Horde generation is not covered yet.

## Resources

- GitHub: <https://github.com/SillyTavern/SillyTavern>
- Docs: <https://docs.sillytavern.app/>
- Discord: <https://discord.gg/sillytavern>
- Reddit: <https://reddit.com/r/SillyTavernAI>

## License

AGPL-3.0
