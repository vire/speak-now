#include <node_api.h>
#include <sys/file.h>
#include <errno.h>

static napi_value lock_guard(napi_env env, napi_callback_info info) {
  size_t argc = 1;
  napi_value args[1];
  napi_get_cb_info(env, info, &argc, args, NULL, NULL);
  int32_t fd;
  napi_get_value_int32(env, args[0], &fd);
  int result = flock(fd, LOCK_EX | LOCK_NB);
  napi_value value;
  napi_create_int32(env, result == 0 ? 0 : (errno == EAGAIN || errno == EWOULDBLOCK ? 1 : errno), &value);
  return value;
}

static napi_value init(napi_env env, napi_value exports) {
  napi_value function;
  napi_create_function(env, "lock", NAPI_AUTO_LENGTH, lock_guard, NULL, &function);
  napi_set_named_property(env, exports, "lock", function);
  return exports;
}

NAPI_MODULE(NODE_GYP_MODULE_NAME, init)
