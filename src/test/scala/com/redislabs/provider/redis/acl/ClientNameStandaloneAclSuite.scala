package com.redislabs.provider.redis.acl

import com.redislabs.provider.redis.ClientNameSuite
import com.redislabs.provider.redis.env.RedisStandaloneAclEnv

/** With an ACL user, the name is set after AUTH user password, on the same connection. */
class ClientNameStandaloneAclSuite extends ClientNameSuite with RedisStandaloneAclEnv
