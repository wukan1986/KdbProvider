using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SmartQuant;
using System.ComponentModel;
using kx;
using NLog;
using static kx.c;

namespace Kan
{
    public partial class KdbProvider : Provider
    {
        protected const string CATEGORY_NETWORK = "Network";
        protected const string CATEGORY_LOGIN = "Login";

        [Category(CATEGORY_NETWORK)]
        public string Host { get; set; }
        [Category(CATEGORY_NETWORK)]
        public int Port { get; set; }
        [Category(CATEGORY_LOGIN), Description("格式 username:password")]
        public string UsernameAndPassword { get; set; }

        // Kdb连接
        private c c = null;

        private Logger Log = LogManager.GetCurrentClassLogger();

        public KdbProvider(Framework framework) : base(framework)
        {
            base.id = 75;
            base.name = "KdbPlus";
            base.description = "Kdb+ Historical";
            base.url = "";

            // 设置默认值
            Host = "127.0.0.1";
            Port = 5001;
        }
        
        protected override void OnConnect()
        {
            if (base.enabled && base.IsDisconnected)
            {
                _Connect();
            }
        }

        protected override void OnDisconnect()
        {
            if (base.IsConnected)
            {
                _Disconnect();
            }
        }

        private void _Connect()
        {
            if (null != c)
            {
                Log.Warn("已经连接上了");
                base.Status = ProviderStatus.Disconnected;
                return;
            }

            try
            {
                base.Status = ProviderStatus.Connecting;
                if (string.IsNullOrEmpty(UsernameAndPassword))
                    c = new c(Host, Port);
                else
                    c = new c(Host, Port, UsernameAndPassword);
                base.Status = ProviderStatus.Connected;
            }
            catch (Exception e)
            {
                base.EmitProviderError(new ProviderError(DateTime.Now, ProviderErrorType.Error, base.id, e.Message));
                Log.Error(e.Message);
                base.Status = ProviderStatus.Disconnected;
                return;
            }
        }

        private void _Disconnect()
        {
            try
            {
                base.Status = ProviderStatus.Disconnecting;
                if (null != c)
                    c.Close();
            }
            catch (Exception e)
            {
                base.EmitProviderError(new ProviderError(DateTime.Now, ProviderErrorType.Error, base.id, e.Message));
                Log.Error(e.Message);
            }
            finally
            {
                c = null;
                base.Status = ProviderStatus.Disconnected;
            }
        }
    }
}
