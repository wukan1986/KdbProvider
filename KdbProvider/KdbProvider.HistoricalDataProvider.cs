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
    public partial class KdbProvider :IHistoricalDataProvider
    {
        private List<DataObject> RequestTrade(HistoricalDataRequest request)
        {
            string query_str = string.Format("select from (select datetime, sym, price, size:{{first[x] -': x}} volume, openint from `volume xasc select from `trade where sym = `$\"{0}\") where size >0", request.Instrument.GetSymbol(base.id));
            Log.Info(query_str);
            Flip flip = (Flip)c.k(query_str);
            int nRows = c.n(flip.y[0]); // flip.y is an array of columns. Get the number of rows from the first column.
            int nColumns = c.n(flip.x);
            var y_datetime = (DateTime[])flip.y[0];
            var y_sym = (string[])flip.y[1];
            var y_price = (double[])flip.y[2];
            var y_size = (long[])flip.y[3];
            var y_openint = (long[])flip.y[4];


            var ts = new List<DataObject>();
            for (int i = 0; i < nRows; ++i)
            {
                var t = new Trade()
                {
                    ProviderId = base.id,
                    InstrumentId = request.Instrument.Id,
                    DateTime = y_datetime[i],
                    Price = y_price[i],
                    Size = (int)y_size[i],
                };
                ts.Add(t);
            }

            return ts;
        }

        private List<DataObject> RequestQuote(HistoricalDataRequest request)
        {
            string query_str = string.Format("`datetime xasc select from `quote where sym = `$\"{0}\"", request.Instrument.GetSymbol(base.id));
            Log.Info(query_str);
            Flip flip = (Flip)c.k(query_str);
            int nRows = c.n(flip.y[0]); // flip.y is an array of columns. Get the number of rows from the first column.
            int nColumns = c.n(flip.x);
            var y_datetime = (DateTime[])flip.y[0];
            var y_sym = (string[])flip.y[1];
            var y_bid = (double[])flip.y[2];
            var y_ask = (double[])flip.y[3];
            var y_bsize = (int[])flip.y[4];
            var y_asize = (int[])flip.y[5];

            var qs = new List<DataObject>();
            for (int i = 0; i < nRows; ++i)
            {
                var q = new Quote(y_datetime[i], base.id, request.Instrument.Id, y_bid[i], y_bsize[i], y_ask[i], y_asize[i]);
                qs.Add(q);
            }
            return qs;
        }

        private List<DataObject> RequestBar(HistoricalDataRequest request)
        {
            if(request.BarType != BarType.Time)
            {
                return new List<DataObject>();
            }

            long barSize = request.BarSize.Value;

            string query_str = string.Format("`datetime xasc select first datetime, last sym, open:first price, high:max price, low:min price, close:last price, volume:sum size, last openint by {0} xbar datetime.second from select datetime, sym, price, size:{{first[x] -': x}} volume, openint from `volume xasc select from `trade where sym = `$\"{1}\"", barSize, request.Instrument.GetSymbol(base.id));
            Log.Info(query_str);
            var result = c.k(query_str);
            Flip flip = c.td(result);
            int nRows = c.n(flip.y[0]); // flip.y is an array of columns. Get the number of rows from the first column.
            int nColumns = c.n(flip.x);
            var y_second = (kx.c.Second[])flip.y[0];
            var y_datetime = (DateTime[])flip.y[1];
            var y_sym = (string[])flip.y[2];
            var y_open = (double[])flip.y[3];
            var y_high = (double[])flip.y[4];
            var y_low = (double[])flip.y[5];
            var y_close = (double[])flip.y[6];
            var y_volume = (long[])flip.y[7];
            var y_openint = (long[])flip.y[8];

            var bs = new List<DataObject>();
            for (int i = 0; i < nRows; ++i)
            {
                long num2 = (((long)y_datetime[i].TimeOfDay.TotalSeconds) / barSize) * barSize;
                var openDatetime = y_datetime[i].Date.AddSeconds(num2);

                var b = new Bar(openDatetime, openDatetime.AddSeconds(barSize), request.Instrument.Id,
                    request.BarType.Value, request.BarSize.Value,
                    y_open[i], y_high[i], y_low[i], y_close[i],
                    y_volume[i],y_openint[i]);
                bs.Add(b);
            }
            return bs;
        }

        public override void Send(HistoricalDataRequest request)
        {
            if (base.IsDisconnected)
            {
                base.EmitHistoricalDataEnd(request.RequestId, RequestResult.Error, "Provider is not connected.");
                Log.Error("Provider is not connected.");
                return;
            }

            var _list = new List<DataObject>();
            switch (request.DataType)
            {
                case DataObjectType.Trade:
                    _list = RequestTrade(request);
                    break;
                case DataObjectType.Quote:
                    _list = RequestQuote(request);
                    break;
                case DataObjectType.Bar:
                    _list = RequestBar(request);
                    break;
            }

            List<DataObject> list = new List<DataObject>();
            for (int i = 0; i < _list.Count; i++)
            {
                DataObject item = _list[i];
                if ((item.DateTime >= request.DateTime1) && (item.DateTime < request.DateTime2))
                {
                    list.Add(item);
                }
            }

            // 对数据过滤一下，然后判断是否有数据
            if (list.Count == 0)
            {
                base.EmitHistoricalDataEnd(request.RequestId, RequestResult.Completed, "No data");
                return;
            }

            if (list.Count > 0)
            {
                HistoricalData data = new HistoricalData
                {
                    RequestId = request.RequestId,
                    Objects = list.ToArray(),
                    TotalNum = list.Count
                };
                base.EmitHistoricalData(data);
            }
            base.EmitHistoricalDataEnd(request.RequestId, RequestResult.Completed, "Completed");

        }

        public void Cancel(string requestId)
        {
        }
    }
}
