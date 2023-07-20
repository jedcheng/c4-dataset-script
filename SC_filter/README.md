# Filter out entries containing simplified Chinese

The list of simplified Chinese characters is from an amazing open source tool [OpenCC](https://github.com/BYVoid/OpenCC/blob/master/data/dictionary/STCharacters.txt)


I then filtered out some simplified Chinese words that are literally the same as traditional Chinese. They made it on the list because of some weird reason. For example, 冬 is the simplified version of both 冬 and 鼕. As a result, the word 冬 made it on the list. 


Also, some very common Cantonese words are technically simplified Chinese such as 吓. I again manually removed them from the list.

