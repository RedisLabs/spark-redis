package redis.clients.jedis

import java.util.{List => JList, Map => JMap}

/**
  * @author The Viet Nguyen
  */
object Implicits {

  implicit class KvPipeline(delegate: Pipeline) {

    def getHashAllWithKey(key: String): Response[(String, JMap[String, String])] = {
      delegate.getClient(key).hgetAll(key)
      delegate.getResponse(new HashKeyBuilderWithKey(key))
    }

    def getHashMultipleWithKey(key: String,
                               fields: String*): Response[(String, JList[String])] = {
      delegate.getClient(key).hmget(key, fields: _*)
      delegate.getResponse(new StringsBuilderWithKey(key))
    }
  }

  class HashKeyBuilderWithKey(key: String)
    extends Builder[(String, JMap[String, String])] {

    override def build(data: Any): (String, JMap[String, String]) =
      key -> BuilderFactory.STRING_MAP.build(data)
  }

  class StringsBuilderWithKey(key: String)
    extends Builder[(String, JList[String])] {

    override def build(data: Any): (String, JList[String]) =
      key -> BuilderFactory.STRING_LIST.build(data)
  }

}
