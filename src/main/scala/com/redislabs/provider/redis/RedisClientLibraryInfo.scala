package com.redislabs.provider.redis

/**
  * Provides library name and version information for CLIENT SETINFO command.
  */
object RedisClientLibraryInfo {

  /**
    * The library name to be sent via CLIENT SETINFO (hardcoded short name)
    */
  val name: String = "sparkr"

  /**
    * The library version to be sent via CLIENT SETINFO
    */
  val version: String = {
    val pkg = RedisClientLibraryInfo.getClass.getPackage
    val ver = if (pkg != null) pkg.getImplementationVersion else null
    if (ver != null) ver else "unknown"
  }

  /**
    * Returns the library name suffix to be used with ClientSetInfoConfig.withLibNameSuffix
    * Format: "-{name}-v{version}"
    */
  def libNameSuffix: String = s"-$name-v$version"
}

