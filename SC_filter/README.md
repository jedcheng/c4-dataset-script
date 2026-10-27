# Filter out entries containing simplified Chinese

The list of simplified Chinese characters is from an amazing open source tool [OpenCC](https://github.com/BYVoid/OpenCC/blob/master/data/dictionary/STCharacters.txt)


I then filtered out some simplified Chinese words that are literally the same as traditional Chinese. They made it on the list because of some weird reason. For example, 冬 is the simplified version of both 冬 and 鼕. As a result, the word 冬 made it on the list. 


Also, some very common Cantonese words are technically simplified Chinese such as 吓. I again manually removed them from the list.


## 2026 fix: 83 Traditional / Cantonese characters removed

The step above was incomplete. On pure Hong Kong Cantonese text (LIHKG posts) the old list flagged ~0.8% of all Han characters as simplified, against a 1% document threshold, so **~33% of Cantonese paragraphs (≥50 chars) and ~7% of whole threads were wrongly rejected**. `出` alone (listed only because it is the simplified form of `齣`) was ~0.5% of all characters.

Removed:

1. Characters OpenCC lists as their own traditional form (48), e.g. `出→出 齣`, `玩→玩 翫`, `背→背 揹`, `夫`, `仇`, `欲`, `梁`, `搜`, `核`, `涌`, `范` … (what the paragraph above intended to remove).
2. Characters that are the standard Hong Kong / Taiwan form according to OpenCC `HKVariants.txt` / `TWVariants.txt` (34), e.g. `温`, `秘`, `痴`, `床`, `群`, `峰`, `税`, `悦`, `灶`, `葱`, `卧`.
3. Very common in Cantonese / HK writing: `晒` (唔該晒, 多謝晒), `麽`, `霉`.

Simplified forms of Cantonese characters (`啰 喽 叽 唛 㖞 揿 闩`) are kept, because HK writing uses `囉 嘍 嘰 嘜 喎 撳 閂`.

Effect on LIHKG (paragraphs ≥50 chars, n=21,589): wrongly flagged 32.6% → 0.22%. The same text converted to simplified with OpenCC is still caught 99.91% of the time, so detection of real simplified text is unchanged.

