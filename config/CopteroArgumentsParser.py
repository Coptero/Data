
class CopteroArguments:
    s3confPath = ""
    s3filePath = ""

class CopteroArgumentsLite:
    s3confPath = ""

class CopteroArgumentsParserLite(scopt.OptionParser[CopteroArgumentsLite]("Coptero")):
    head("Coptero", "1.0.0")
    help("help").text("Print this usage text")
    version("version").text("Show version")

    opt[String]("conf")\
        .required()\
        .valueName("<conf>")
        .action((x, c) = > c.copy(s3confPath=x)).text("s3 conf file path")

