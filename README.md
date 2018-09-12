## BIG-BANG PROTOTYPE

欢迎来到特征大爆炸现场，特征工程的制作现场太需要一台傻瓜烹饪机，以用来承载茶米酱醋盐随机组合带来的美味佳肴。
这个过程是及其枯燥但又不的不进行的，模型算法，抛去算法本身的特性研究那块，本质上是一门侧头侧尾的实验科学。

爱迪生老爷爷说天才是1%的灵感+99%的汗水，BIG-BANG的初心就是把大家在灵感的时间投入更多，让机器帮人承载原先同学们99%的一遍又一遍枯燥乏味的变量组装过程。

当然没有银弹，BIG-BANG从设计至今有已经沉淀思考了不少时间，一份粗制的备忘录如下:

- BIG-BANG不会轻易在领域模型上增加复杂的组合维度
- BIG-BANG的原初材料是一份原始数据魔方，这个魔方应该是被精心准备的，做了基本的数据过滤，字段增补的大宽表。
任何试图在BIG-BANG里面来玩表与表之间join过程的都是耍流氓，这只会将变量衍生的便捷无限外放，以至于超出控制。
- BIG-BANG大致分两步进行，第一步是将宽表根据切分条件，增广若干列控制基元变量，并将数据歇会数仓
- BIG-BANG在不同的分割组之间是组合关系，当组合关系选定之后，内部元素之间是笛卡尔积关系
- BIG-BANG必须要有聚合过程
- BIG-BANG在特征聚合过程时，将采用parameter-server的模式，采用分布任务的方式将特征分组加工，eg每组加工5000个特征, 这些特征都将写入数仓
- BIG-BANG很容易做到将其他分组条件固定，只保留某一组可变的Seprator级别的两两除法.  eg(A条件下B条件下C条件取值1与2计算值的比例, 过去1个月拨出电话与过去3个月拨出电话的比例)
- BIG-BANG目前还没有计划做组内排序的过滤，这是因为普通的过滤模式下顺序是无关的，因此它是一个组合问题，如果做组内排序，那么插入的排序与过滤会与顺序有关，它将变成一个排列问题，复杂度急剧增加
- BIG-BANG可以与变量中心很好兼容，BIGBANG可以追溯任何一个特征计算当时的sql表达式，并且一个特征一条，互不干扰
- BIG-BANG的变量名足够涵盖特征加工过程，可读性容易控制
- BIG-BANG背后是spark计算引擎，性能好过不经过精细调制的一般编码规范，由于它是框架性质，因此任何性能的优化只需要在框架做一遍即可永久享受
- BIG BIG BIG ...


终于结束万某人特色的废话环节，来到DEMO

## DEMO

程序的入口为:

com.wacai.stanlee.launcher.BigBangMain main方法。执行即可看到过程，
注意请修改文件路径，（bug后续会修复）
其中数据是随机MOCK的

demo的核心要素就是, 在resources文件夹中bang.json是变量衍生的核心，它意味着只要大家定义好一个分片聚合的元数据，接下来程序会自动做组合，笛卡尔积交叉

- 比如在aggGroupList中增加聚合的字段，它会根据字段的类型，自动适配聚合的方式
- 比如在sepGroupList中可以增加或者修改切割的条件，
- 高阶玩法： 比如修改CellCallDO对象，加入新的字段，并填充数据，然后做切片

```json
{
  "aggGroupList": [
    {
      "name": "拨打电话个数",
      "target": "ortherPhone"
    }
  ],
  "creator": "万两",
  "maxCombination": 5,
  "sepGroupList": [
    {
      "macro": "months_between(startTime, currentTime) \\< <val>",
      "name": "月份切割",
      "seprators": [
        {
          "desc": "距今3个月之内",
          "expression": "#macro({\"val\": 3})",
          "name": "lg3m"
        },
        {
          "desc": "距今2个月之内",
          "expression": "#macro({\"val\": 2})",
          "name": "lg2m"
        },
        {
          "desc": "距今1个月之内",
          "expression": "#macro({\"val\": 1})",
          "name": "lg1m"
        }
      ],
      "targetFields": [
        "startTime",
        "currentTime"
      ]
    },
    {
      "name": "上中下旬",
      "seprators": [
        {
          "name": "first10",
          "desc": "first ten-day of the month",
          "expression": "(day(startTime) >=1 and day(startTIME) < 10)"
        },
        {
          "name": "mid10",
          "desc": "the middle ten-day of a month",
          "expression": "(day(startTime) >=10 and day(startTIME) < 20)"
        },
        {
          "name": "last10",
          "desc": "the last ten-day of a month",
          "expression": "(day(startTime) >=20 and day(startTIME) < 31)"
        }
      ]
    }
  ],
  "table": {
    "desc": "运营商表",
    "fields": [
      {
        "fieldName": "phone",
        "fieldType": "STRING"
      },
      {
        "fieldName": "ortherPhone",
        "fieldType": "STRING"
      },
      {
        "fieldName": "startTime",
        "fieldType": "DATE"
      },
      {
        "fieldName": "currentTime",
        "fieldType": "DATE"
      },
      {
        "fieldName": "aid",
        "fieldType": "NUMERIC"
      }
    ],
    "name": "cell_call"
  }
}
```

SCREENSHOT

![image](http://git.caimi-inc.com/ml.wac/big-bang/uploads/4449cdd5459b1884c97fee9f44dfd3b6/image.png)

1. 编译 
```shell
mvn clean package -DskipTests
```
2. 启动

```shell
java -classpath "target/big-bang-1.0-SNAPSHOT.jar:target/lib/*" com.wacai.stanlee.launcher.BigBangMain
```