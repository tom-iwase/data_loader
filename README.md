# data_loader

## 使い方

data_loader.pyの中に、読み込みたいデータの辞書を追加してください。
```python
'Key':{
     'path_format' : "PATH",                                                                
     'url_format': "URL",                                                                     
     'load_method': {'load_func': "FUNCTION", 'load_args': "{""ARG01"": ""VALUE01"", ...}"},
     'auth': {'auth_func': "FUNCTION", 'auth_args': "{""ARG01"": ""VALUE01"", ...}"}          
}
```

- path_format: ファイルのパスのフォーマット。datetime.strftime()を用いて日付ごとにパスが生成できるようにする。
- url_format: ファイルのURLのフォーマット。datetime.strftime()を用いて日付ごとにURLが生成できるようにする。
- load_method: ローカルに保存したファイルを読み込むときに必要なメソッドと引数を設定する。
- auth: ファイルのダウンロード時に認証が必要な場合に設定する。認証によって使用するメソッドが異なる。requestを参照。

DataLoaderクラスをdata_loader.pyからimportする。
```python
from data_loader import DataLoader
```

DataLoader()のインスタンスを、"key"と"datetime(必要なら)"の引数から作成する。
```python
data = DataLoader('calet_chd', '2019-01-01')
```

path,とurlが確認できる。
```python
print('path: ', data.path)
print('url: ', data.url)
```
```
> path:  /mnt/d/research/data/calet/cal-v1.1/CHD/level1.1/obs/2019/CHD_190101.dat
> url:  https://data.darts.isas.jaxa.jp/pub/calet/cal-v1.1/CHD/level1.1/obs/2019/CHD_190101.dat
```

dl()メソッドでデータをダウンロードする。
- dl()はダウンロードの結果を返す(True/False)。
- データは辞書で設定されたパスに保存される。
```python
data.dl()
#overwritingがTrueならファイルは上書きされる。
#data.dl(overwriting=True)
```
```
> [info] save() : calet_chd 2019-01-01 00:00:00 data : file exists and overwriting is False
```

設定されたパスにデータがあれば、load()メソッドはデータを返す。
```python
data.load()

#dl(download)がTrueかつファイルが存在しないとき、ファイルをダウンロードした上でデータを返す。
#data.load(dl=True)
```
```

	0 	1 	2 	3 	4 	5 	6 	7
0 	1.546301e+09 	1.000000 	1306 	1331 	-48.17 	253.80 	420.65 	2
1 	1.546301e+09 	1.000000 	1291 	1290 	-48.19 	253.89 	420.66 	2
2 	1.546301e+09 	1.000125 	1218 	1265 	-48.22 	253.97 	420.67 	2
3 	1.546301e+09 	1.002312 	1358 	1334 	-48.24 	254.06 	420.68 	2
4 	1.546301e+09 	0.997687 	1312 	1311 	-48.26 	254.14 	420.69 	2
... 	... 	... 	... 	... 	... 	... 	... 	...
86344 	1.546387e+09 	0.996812 	2405 	2385 	51.75 	93.19 	413.30 	2
86345 	1.546387e+09 	1.007187 	2345 	2351 	51.76 	93.29 	413.30 	2
86346 	1.546387e+09 	0.996375 	2275 	2283 	51.76 	93.39 	413.30 	2
86347 	1.546387e+09 	1.007562 	2279 	2241 	51.76 	93.49 	413.30 	2
86348 	1.546387e+09 	0.988938 	2370 	2381 	51.76 	93.59 	413.30 	2

6349 rows × 8 columns
```

load()したデータはcontent引数内に格納されている。
```python
data.content
```
