package redis.clients.jedis

import java.util.{Map => JMap}

/**
  * @author The Viet Nguyen
  */
object Implicits {

  implicit class KvPipeline(delegate: Pipeline) {

    def getHashAllWithKey(key: String): Response[(String, JMap[String, String])] = {
      delegate.getClient(key).hgetAll(key)
      delegate.getResponse(new HashKeyBuilderWithKey(key))
    }
  }

  class HashKeyBuilderWithKey(key: String)
    extends Builder[(String, JMap[String, String])] {

    override def build(data: Any): (String, JMap[String, String]) =
      key -> BuilderFactory.STRING_MAP.build(data)
  }
}
