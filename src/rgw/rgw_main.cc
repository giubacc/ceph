// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <boost/intrusive/list.hpp>
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "global/signal_handler.h"
#include "common/config.h"
#include "common/errno.h"
#include "common/Timer.h"
#include "common/TracepointProvider.h"
#include "rgw_main.h"
#include "rgw_signal.h"
#include "rgw_common.h"
#include "rgw_lib.h"
#include "rgw_log.h"

#ifdef HAVE_SYS_PRCTL_H
#include <sys/prctl.h>
#endif

using namespace std;

static constexpr auto dout_subsys = ceph_subsys_rgw;

static sig_t sighandler_alrm;

static void godown_alarm(int signum)
{
  _exit(0);
}

class C_InitTimeout : public Context {
public:
  C_InitTimeout() {}
  void finish(int r) override {
    derr << "Initialization timeout, failed to initialize" << dendl;
    exit(1);
  }
};

void mark_die_evt(const char *how)
{
  uint64_t ts = std::chrono::duration_cast<std::chrono::nanoseconds>(ceph::real_clock::now().time_since_epoch()).count();
  FILE *dth_fd = nullptr;
  std::ostringstream os;
  os << g_conf().get_val<std::string>("rgw_data") << '/' << "DeathToken";
  if((dth_fd = fopen(os.str().c_str(), "w"))) {
    fprintf(dth_fd, "%lu\n%s", ts, how);
    fclose(dth_fd);
  }
}

uint64_t ts_main = 0;

void send_probe_start_evt(const char *where, uint64_t ts, const boost::intrusive_ptr<CephContext> &cct)
{
  NoDoutPrefix ndp(g_ceph_context, 1);

  RGWAccessKey key;
  key.id = "test";
  key.key = "test";

  RGWEnv env;
  req_info info(g_ceph_context, &env);
  info.method = "PUT";
  info.request_uri = "/start";

  param_vec_t params;

  std::string endpoint = g_conf().get_val<std::string>("probe_endpoint");
  ldout(cct, 0) << "[start evt] endpoint:" << endpoint << dendl;

  std::ostringstream os;
  os << ts;

  params.push_back(param_pair_t("ts", os.str()));
  params.push_back(param_pair_t("where", where));
  RGWRESTSimpleRequest req(g_ceph_context, "PUT", endpoint, NULL, &params, std::nullopt);

  bufferlist response;
  int ret = req.forward_request(&ndp, key, info, 1024, NULL, &response, null_yield);
  if(ret){
      ldout(cct, 0) << "forward_request failed:" << ret << dendl;
  }
}

void send_probe_death_evt(const char *how, const char *ts, const boost::intrusive_ptr<CephContext> &cct)
{
  NoDoutPrefix ndp(g_ceph_context, 1);

  RGWAccessKey key;
  key.id = "test";
  key.key = "test";

  RGWEnv env;
  req_info info(g_ceph_context, &env);
  info.method = "PUT";
  info.request_uri = "/death";

  param_vec_t params;
  params.push_back(param_pair_t("type", how));

  std::string endpoint = g_conf().get_val<std::string>("probe_endpoint");
  ldout(cct, 0) << "[death evt] endpoint:" << endpoint << dendl;

  params.push_back(param_pair_t("ts", ts));
  RGWRESTSimpleRequest req(g_ceph_context, "PUT", endpoint, NULL, &params, std::nullopt);

  bufferlist response;
  int ret = req.forward_request(&ndp, key, info, 1024, NULL, &response, null_yield);
  if(ret){
      ldout(cct, 0) << "forward_request failed:" << ret << dendl;
  }
}

static int usage()
{
  cout << "usage: radosgw [options...]" << std::endl;
  cout << "options:\n";
  cout << "  --rgw-region=<region>     region in which radosgw runs\n";
  cout << "  --rgw-zone=<zone>         zone in which radosgw runs\n";
  cout << "  --rgw-socket-path=<path>  specify a unix domain socket path\n";
  cout << "  -m monaddress[:port]      connect to specified monitor\n";
  cout << "  --keyring=<path>          path to radosgw keyring\n";
  cout << "  --logfile=<logfile>       file to log debug output\n";
  cout << "  --debug-rgw=<log-level>/<memory-level>  set radosgw debug level\n";
  generic_server_usage();

  return 0;
}

/*
 * start up the RADOS connection and then handle HTTP messages as they come in
 */
int main(int argc, char *argv[])
{
  /****************************
   * ts_main
  ****************************/
  ts_main = std::chrono::duration_cast<std::chrono::nanoseconds>(ceph::real_clock::now().time_since_epoch()).count();

  int r{0};

  // dout() messages will be sent to stderr, but FCGX wants messages on stdout
  // Redirect stderr to stdout.
  TEMP_FAILURE_RETRY(close(STDERR_FILENO));
  if (TEMP_FAILURE_RETRY(dup2(STDOUT_FILENO, STDERR_FILENO)) < 0) {
    int err = errno;
    cout << "failed to redirect stderr to stdout: " << cpp_strerror(err)
         << std::endl;
    return ENOSYS;
  }

  /* alternative default for module */
  map<std::string,std::string> defaults = {
    { "debug_rgw", "1/5" },
    { "keyring", "$rgw_data/keyring" },
    { "objecter_inflight_ops", "24576" },
    // require a secure mon connection by default
    { "ms_mon_client_mode", "secure" },
    { "auth_client_required", "cephx" }
  };

  auto args = argv_to_vec(argc, argv);
  if (args.empty()) {
    cerr << argv[0] << ": -h or --help for usage" << std::endl;
    exit(1);
  }
  if (ceph_argparse_need_usage(args)) {
    usage();
    exit(0);
  }

  int flags = CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS;
  // Prevent global_init() from dropping permissions until frontends can bind
  // privileged ports
  flags |= CINIT_FLAG_DEFER_DROP_PRIVILEGES;

  auto cct = rgw_global_init(&defaults, args, CEPH_ENTITY_TYPE_CLIENT,
			     CODE_ENVIRONMENT_DAEMON, flags);

  DoutPrefix dp(cct.get(), dout_subsys, "rgw main: ");
  rgw::AppMain main(&dp);

  main.init_frontends1(false /* nfs */);
  main.init_numa();

  if (g_conf()->daemonize) {
    global_init_daemonize(g_ceph_context);
  }
  ceph::mutex mutex = ceph::make_mutex("main");
  SafeTimer init_timer(g_ceph_context, mutex);
  init_timer.init();
  mutex.lock();
  init_timer.add_event_after(g_conf()->rgw_init_timeout, new C_InitTimeout);
  mutex.unlock();

  common_init_finish(g_ceph_context);
  init_async_signal_handler();

  /* XXXX check locations thru sighandler_alrm */
  register_async_signal_handler(SIGHUP, rgw::signal::sighup_handler);
  r = rgw::signal::signal_fd_init();
  if (r < 0) {
    derr << "ERROR: unable to initialize signal fds" << dendl;
  exit(1);
  }

  register_async_signal_handler(SIGTERM, rgw::signal::handle_sigterm);
  register_async_signal_handler(SIGINT, rgw::signal::handle_sigterm);
  register_async_signal_handler(SIGUSR1, rgw::signal::handle_sigterm);
  sighandler_alrm = signal(SIGALRM, godown_alarm);

  main.init_perfcounters();
  main.init_http_clients();

  main.init_storage();
  if (! main.get_driver()) {
    mutex.lock();
    init_timer.cancel_all_events();
    init_timer.shutdown();
    mutex.unlock();

    derr << "Couldn't init storage provider (RADOS)" << dendl;
    return EIO;
  }

  main.init_s3gw_telemetry();
  main.cond_init_apis();

  mutex.lock();
  init_timer.cancel_all_events();
  init_timer.shutdown();
  mutex.unlock();

  main.init_ldap();
  main.init_opslog();
  main.init_tracepoints();
  main.init_lua();
  main.init_frontends2(nullptr /* RGWLib */);
  main.init_notification_endpoints();

#if defined(HAVE_SYS_PRCTL_H)
  if (prctl(PR_SET_DUMPABLE, 1) == -1) {
    cerr << "warning: unable to set dumpable flag: " << cpp_strerror(errno) << std::endl;
  }
#endif

  rgw::signal::wait_shutdown();
  mark_die_evt("regular");

  derr << "shutting down" << dendl;

  const auto finalize_async_signals = []() {
    unregister_async_signal_handler(SIGHUP, rgw::signal::sighup_handler);
    unregister_async_signal_handler(SIGTERM, rgw::signal::handle_sigterm);
    unregister_async_signal_handler(SIGINT, rgw::signal::handle_sigterm);
    unregister_async_signal_handler(SIGUSR1, rgw::signal::handle_sigterm);
    shutdown_async_signal_handler();
  };

  main.shutdown(finalize_async_signals);

  dout(1) << "final shutdown" << dendl;

  rgw::signal::signal_fd_finalize();

  return 0;
} /* main(int argc, char* argv[]) */
