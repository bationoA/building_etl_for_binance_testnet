from config import config
import datetime
import urllib
import mysql.connector

from binance.client import Client, BaseClient

import pandas as pd
import requests
import json
import talib as tb


def make_request(url: str, data, headers: dict) -> json:
    response = requests.post(url, json=data, headers=headers)
    return response


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """

    :param df:
    :return:
    """
    # ---------- Constants
    periods_9_14_24 = [9, 14, 24]
    periods_5_10_20_50 = [5, 10, 20, 50]
    periods_5_10_20_30_50 = [5, 10, 20, 30, 50]
    periods_10_30_50_100_200 = [10, 30, 50, 100, 200]
    periods_9_18_27_36 = [9, 18, 27, 36]
    periods_7_14_28_56 = [7, 14, 28, 56]
    # ----------------------

    _df = df.copy(deep=True)

    # # Overlap Studies

    # ### BBANDS - Bollinger Bands

    # In[123]:

    for period in periods_5_10_20_50:
        _df[f'BBANDS_upperband_{period}'], _df[f'BBANDS_middleband_{period}'], _df[
            f'BBANDS_lowerband_{period}'] = tb.BBANDS(_df['Close'], timeperiod=period, nbdevup=2, nbdevdn=2,
                                                      matype=0)

    # ### DEMA - Double Exponential Moving Average

    # In[125]:

    for period in periods_5_10_20_30_50:
        _df[f'DEMA_{period}'] = tb.DEMA(_df['Close'], timeperiod=period)

    # ### EMA - Exponential Moving Average

    # In[127]:

    for period in periods_10_30_50_100_200:
        _df[f'EMA_{period}'] = tb.EMA(_df['Close'], timeperiod=period)

    # In[128]:

    _df = _df.copy(deep=True)

    # ### HT_TRENDLINE - Hilbert Transform - Instantaneous Trendline

    # In[129]:

    _df[f'HT_TRENDLINE'] = tb.EMA(_df['Close'])

    # ### KAMA - Kaufman Adaptive Moving Average

    # In[130]:

    for period in periods_5_10_20_30_50:
        _df[f'KAMA_{period}'] = tb.KAMA(_df['Close'], timeperiod=period)

    # ### MA - Moving average

    # In[131]:

    for period in periods_5_10_20_30_50:
        _df[f'MA_{period}'] = tb.MA(_df['Close'], timeperiod=period)

    # ### MAMA - MESA Adaptive Moving Average

    # In[136]:

    _df[f'MAMA_mama'], _df[f'MAMA_fama'] = tb.MAMA(
        _df['Close'])  # Adding , fastlimit=0, slowlimit=0 raises an error

    # ### MAVP - Moving average with variable period

    # In[146]:

    # close, periods
    _df['MAVP'] = tb.MAVP(_df['Close'], _df['Open_time'], minperiod=2, maxperiod=30, matype=0)

    # ### MIDPOINT - MidPoint over period

    # In[147]:

    for period in periods_9_14_24:
        _df[f'MIDPOINT_{period}'] = tb.MIDPOINT(_df['Close'], timeperiod=period)

    # ### MIDPRICE - Midpoint Price over period

    # In[148]:

    for period in periods_9_14_24:
        _df[f'MIDPRICE_{period}'] = tb.MIDPRICE(high=_df['High'], low=_df['Low'], timeperiod=period)

    # ### SAR - Parabolic SAR

    # In[150]:

    _df[f'SAR'] = tb.MIDPRICE(high=_df['High'],
                              low=_df['Low'])  # Adding , acceleration=0, maximum=0 raises an error

    # ### SAREXT - Parabolic SAR - Extended

    # In[151]:

    _df[f'SAREXT'] = tb.SAREXT(high=_df['High'], low=_df['Low'], startvalue=0, offsetonreverse=0,
                               accelerationinitlong=0, accelerationlong=0, accelerationmaxlong=0,
                               accelerationinitshort=0, accelerationshort=0, accelerationmaxshort=0)

    _df = _df.copy(deep=True)

    # ### SMA - Simple Moving Average

    # In[152]:

    for period in periods_5_10_20_30_50:
        _df[f'SMA_{period}'] = tb.SMA(_df['Close'], timeperiod=period)

    # ### T3 - Triple Exponential Moving Average (T3)

    # In[153]:

    for period in periods_5_10_20_30_50:
        _df[f'T3_{period}'] = tb.T3(_df['Close'], timeperiod=period, vfactor=0)

    # ### TEMA - Triple Exponential Moving Average

    # In[154]:

    for period in periods_5_10_20_30_50:
        _df[f'TEMA_{period}'] = tb.TEMA(_df['Close'], timeperiod=period)

    # ### TRIMA - Triangular Moving Average

    # In[155]:

    for period in periods_5_10_20_30_50:
        _df[f'TRIMA_{period}'] = tb.TRIMA(_df['Close'], timeperiod=period)

    # ### WMA - Weighted Moving Average

    # In[156]:

    for period in periods_5_10_20_30_50:
        _df[f'WMA_{period}'] = tb.WMA(_df['Close'], timeperiod=period)

    # # Momentum Indicator Functions

    # ### Average Directional Movement Index

    # In[50]:

    for period in periods_9_14_24:
        _df[f'ADX_{period}'] = tb.ADX(high=_df['High'], low=_df['Low'], close=_df['Close'], timeperiod=period)

    # ### ADXR - Average Directional Movement Index Rating

    # In[51]:

    for period in periods_9_14_24:
        _df[f'ADXR_{period}'] = tb.ADXR(high=_df['High'], low=_df['Low'], close=_df['Close'], timeperiod=period)

    # ### APO - Absolute Price Oscillator

    _df = _df.copy(deep=True)

    # In[55]:

    _df[f'APO'] = tb.APO(_df['Close'], fastperiod=12, slowperiod=26, matype=0)

    # ### AROON - Aroon

    # In[56]:

    for period in periods_9_14_24:
        _df[f'AROON_down'], _df[f'AROON_up'] = tb.AROON(high=_df['High'], low=_df['Low'], timeperiod=period)

    # ### AROONOSC - Aroon Oscillator

    # In[58]:

    for period in periods_9_14_24:
        _df[f'AROON_oscillator'] = tb.AROONOSC(high=_df['High'], low=_df['Low'], timeperiod=period)

    # ### BOP - Balance Of Power

    # In[59]:

    _df[f'BOP'] = tb.BOP(open=_df['Open'], high=_df['High'], low=_df['Low'], close=_df['Close'])

    # ### CCI - Commodity Channel Index

    # In[61]:

    for period in periods_9_14_24:
        _df[f'CCI_{period}'] = tb.CCI(high=_df['High'], low=_df['Low'], close=_df['Close'], timeperiod=period)

    # ### CMO - Chande Momentum Oscillator

    # In[62]:

    for period in periods_9_14_24:
        _df[f'CMO_{period}'] = tb.CMO(_df['Close'], timeperiod=period)

    # ### DX - Directional Movement Index

    # In[63]:

    for period in periods_9_14_24:
        _df[f'DX_{period}'] = tb.DX(high=_df['High'], low=_df['Low'], close=_df['Close'], timeperiod=period)

    _df = _df.copy(deep=True)

    # ### MACD - Moving Average Convergence/Divergence

    # In[66]:

    for period in periods_9_18_27_36:
        _df[f'MACD_{period}'], _df[f'MACD_signal_{period}'], _df[f'MACD_hist_{period}'] = tb.MACD(_df['Close'],
                                                                                                  fastperiod=12,
                                                                                                  slowperiod=26,
                                                                                                  signalperiod=period)

    # ### MACDEXT - MACD with controllable MA type

    # In[67]:

    for period in periods_9_18_27_36:
        _df[f'MACDEXT_{period}'], _df[f'MACDEXT_signal_{period}'], _df[f'MACDEXT_hist_{period}'] = tb.MACDEXT(
            _df['Close'], fastperiod=12, fastmatype=0, slowperiod=26, slowmatype=0, signalperiod=period,
            signalmatype=0)

    # ### MACDFIX - Moving Average Convergence/Divergence Fix 12/26

    # In[68]:

    for period in periods_9_18_27_36:
        _df[f'MACDFIX_{period}'], _df[f'MACDFIX_signal_{period}'], _df[f'MACDFIX_hist_{period}'] = tb.MACDFIX(
            _df['Close'], signalperiod=period)

    # ### MFI - Money Flow Index

    # In[71]:

    for period in periods_9_14_24:
        _df[f'MFI_{period}'] = tb.MFI(high=_df['High'], low=_df['Low'], close=_df['Close'], volume=_df['Volume'],
                                      timeperiod=period)

    # ### MINUS_DI - Minus Directional Indicator

    # In[72]:

    for period in periods_9_14_24:
        _df[f'MINUS_DI_{period}'] = tb.MINUS_DI(high=_df['High'], low=_df['Low'], close=_df['Close'],
                                                timeperiod=period)

    # ### MINUS_DM - Minus Directional Movement

    # In[73]:

    for period in periods_9_14_24:
        _df[f'MINUS_DM_{period}'] = tb.MINUS_DM(high=_df['High'], low=_df['Low'], timeperiod=period)

    _df = _df.copy(deep=True)

    # ### MOM - Momentum

    # In[75]:

    for period in [10] + periods_9_14_24:
        _df[f'MOM_{period}'] = tb.MOM(_df['Close'], timeperiod=period)

    # ### PLUS_DI - Plus Directional Indicator

    # In[76]:

    for period in periods_9_14_24:
        _df[f'PLUS_DI_{period}'] = tb.PLUS_DI(high=_df['High'], low=_df['Low'], close=_df['Close'],
                                              timeperiod=period)

    # ### PLUS_DM - Plus Directional Movement

    # In[77]:

    for period in periods_9_14_24:
        _df[f'PLUS_DM_{period}'] = tb.PLUS_DM(high=_df['High'], low=_df['Low'], timeperiod=period)

    # ### PPO - Percentage Price Oscillator

    # In[79]:

    _df[f'PPO'] = tb.PPO(_df['Close'], fastperiod=12, slowperiod=26, matype=0)

    # ### ROC - Rate of change : ((price/prevPrice)-1)*100

    # In[80]:

    for period in periods_5_10_20_50:
        _df[f'ROC_{period}'] = tb.ROC(_df['Close'], timeperiod=period)

    # At this point, DataFrame _df has been higly fragmented due to too many internal calls of 'frame.insert',
    # which has poo performance. Join all columns at once or #### de-fragmented frame, using `newframe = frame.copy()`

    # In[83]:

    # de-fragmented frame, using `newframe = frame.copy()`
    _df = _df.copy(deep=True)

    # ### ROCP - Rate of change Percentage: (price-prevPrice)/prevPrice

    # In[84]:

    for period in periods_5_10_20_50:
        _df[f'ROCP_{period}'] = tb.ROCP(_df['Close'], timeperiod=period)

    # ### ROCR - Rate of change ratio: (price/prevPrice)

    # In[85]:

    for period in periods_5_10_20_50:
        _df[f'ROCR_{period}'] = tb.ROCR(_df['Close'], timeperiod=period)

    # ### ROCR100 - Rate of change ratio 100 scale: (price/prevPrice)*100

    # In[86]:

    for period in periods_5_10_20_50:
        _df[f'ROCR100_{period}'] = tb.ROCR100(_df['Close'], timeperiod=period)

    # ### RSI - Relative Strength Index

    # In[90]:

    for period in periods_9_14_24:
        _df[f'RSI_{period}'] = tb.RSI(_df['Close'], timeperiod=period)

    # ### STOCH - Stochastic

    # In[91]:

    for period in periods_5_10_20_50:
        # put period-2 just to make them start at their default value
        _df[f'STOCH_slowk_{period}'], _df[f'STOCH_slowd_{period}'] = tb.STOCH(high=_df['High'], low=_df['Low'],
                                                                              close=_df['Close'],
                                                                              fastk_period=period,
                                                                              slowk_period=period - 2,
                                                                              slowk_matype=0,
                                                                              slowd_period=period - 2,
                                                                              slowd_matype=0)

    # ### STOCHF - Stochastic Fast

    # In[93]:

    for period in periods_5_10_20_50:
        # put period-2 just to make them start at their default value
        _df[f'STOCH_fastk_{period}'], _df[f'STOCH_fastd_{period}'] = tb.STOCHF(high=_df['High'], low=_df['Low'],
                                                                               close=_df['Close'],
                                                                               fastk_period=period,
                                                                               fastd_period=period - 2,
                                                                               fastd_matype=0)

    # In[94]:

    # de-fragmented frame, using `newframe = frame.copy()`
    _df = _df.copy(deep=True)

    # ### STOCHRSI - Stochastic Relative Strength Index

    # In[101]:

    for time_period in periods_9_14_24:
        for period in periods_5_10_20_50:
            # put period-2 just to make them start at their default value
            _df[f'STOCHRSI_fastk_{time_period}_{period}'], _df[
                f'STOCHRSI_fastd_{time_period}_{period}'] = tb.STOCHRSI(
                _df['Close'], timeperiod=time_period,
                fastk_period=period, fastd_period=period - 2, fastd_matype=0)

    # ### TRIX - 1-day Rate-Of-Change (ROC) of a Triple Smooth EMA

    # In[104]:

    for period in periods_9_14_24 + [30]:  # Added 30 just to include default value
        _df[f'TRIX_{period}'] = tb.TRIX(_df['Close'], timeperiod=period)

    # ### ULTOSC - Ultimate Oscillator

    # In[111]:

    _period = periods_7_14_28_56.copy()
    for i in range(len(periods_7_14_28_56)):
        _df[f'ULTOSC_{_period[i]}_{_period[i + 1]}_{_period[i + 2]}'] = tb.ULTOSC(high=_df['High'], low=_df['Low'],
                                                                                  close=_df['Close'],
                                                                                  timeperiod1=_period[i],
                                                                                  timeperiod2=_period[i + 1],
                                                                                  timeperiod3=_period[i + 2])
        if i + 2 == len(periods_7_14_28_56) - 1:  # reched last index
            break

    # ### WILLR - Williams' %R

    # In[120]:

    for period in periods_9_14_24:
        _df[f'WILLR_{period}'] = tb.WILLR(high=_df['High'], low=_df['Low'], close=_df['Close'], timeperiod=period)

    # de-fragmented frame, using `newframe = frame.copy()`
    _df = _df.copy(deep=True)

    # In[121]:

    # # Volume Indicators

    # ### AD - Chaikin A/D Line

    # In[162]:

    _df[f'AD_Chaikin_AD_line'] = tb.AD(high=_df['High'], low=_df['Low'], close=_df['Close'], volume=_df['Volume'])

    # ### ADOSC - Chaikin A/D Oscillator

    # In[163]:

    _df[f'ADOSC_Chaikin_AD_oscillator'] = tb.ADOSC(high=_df['High'], low=_df['Low'], close=_df['Close'],
                                                   volume=_df['Volume'], fastperiod=3, slowperiod=10)

    # ### OBV - On Balance Volume

    # In[165]:

    _df[f'OBV_on_balance_volume'] = tb.OBV(_df['Close'], _df['Volume'])

    # # Volatility Indicators

    # ### ATR - Average True Range

    # In[168]:

    for period in periods_9_14_24:
        _df[f'ATR_{period}'] = tb.ATR(high=_df['High'], low=_df['Low'], close=_df['Close'], timeperiod=period)

    # ### NATR - Normalized Average True Range

    # In[169]:

    for period in periods_9_14_24:
        _df[f'NATR_{period}'] = tb.NATR(high=_df['High'], low=_df['Low'], close=_df['Close'], timeperiod=period)

    # ### TRANGE - True Range

    # In[171]:

    _df[f'TRANGE'] = tb.TRANGE(high=_df['High'], low=_df['Low'], close=_df['Close'])

    # # Price Transform Functions

    # de-fragmented frame, using `newframe = frame.copy()`
    _df = _df.copy(deep=True)

    # ### Add Average Price?

    # In[17]:

    _df['Avg_price'] = tb.AVGPRICE(open=_df['Open'],
                                   high=_df['High'],
                                   low=_df['Low'],
                                   close=_df['Close'])
    _df.head()

    # ### Add Median Price

    # In[44]:

    _df['Med_price'] = tb.MEDPRICE(high=_df['High'], low=_df['Low'])

    # ### Typical Price

    # In[45]:

    _df['Typical_price'] = tb.TYPPRICE(high=_df['High'], low=_df['Low'], close=_df['Close'])

    # ### Weighted Close Price

    # In[46]:

    _df['Weighted_close_price'] = tb.WCLPRICE(high=_df['High'], low=_df['Low'], close=_df['Close'])

    # In[ ]:

    # ### Adding SMA

    # In[30]:

    for period in periods_5_10_20_50:
        _df[f'SMA_{period}'] = tb.SMA(_df['Close'], timeperiod=period)

    # ### Adding EMA

    # In[31]:

    for period in periods_5_10_20_50:
        _df[f'EMA_{period}'] = tb.EMA(_df['Close'], timeperiod=period)

    # # Cycle Indicators

    # ### HT_DCPERIOD - Hilbert Transform - Dominant Cycle Period

    # In[173]:

    _df[f'HT_DCPERIOD'] = tb.HT_DCPERIOD(_df['Close'])

    # ### HT_DCPHASE - Hilbert Transform - Dominant Cycle Phase

    # In[174]:

    _df[f'HT_DCPHASE'] = tb.HT_DCPHASE(_df['Close'])

    # ### HT_PHASOR - Hilbert Transform - Phasor Components

    # In[176]:

    _df[f'HT_PHASOR_inphase'], _df[f'HT_PHASOR_quadrature'] = tb.HT_PHASOR(_df['Close'])

    # ### HT_SINE - Hilbert Transform - SineWave

    # In[177]:

    _df[f'HT_SINE_sine'], _df[f'HHT_SINE_leadsine'] = tb.HT_SINE(_df['Close'])

    # ### HT_TRENDMODE - Hilbert Transform - Trend vs Cycle Mode

    # In[178]:

    _df[f'HT_TRENDMODE'] = tb.HT_TRENDMODE(_df['Close'])

    # de-fragmented frame, using `newframe = frame.copy()`
    _df = _df.copy(deep=True)

    # # Pattern Recognition

    # ### CDL2CROWS - Two Crows

    # In[181]:

    _df['CDL2CROWS'] = tb.CDL2CROWS(open=_df['Open'], high=_df['High'], low=_df['Low'], close=_df['Close'])

    # ### CDL3BLACKCROWS - Three Black Crows

    # In[182]:

    _df['CDL3BLACKCROWS'] = tb.CDL3BLACKCROWS(open=_df['Open'], high=_df['High'], low=_df['Low'],
                                              close=_df['Close'])

    # ### CDL3INSIDE - Three Inside Up/Down

    # In[183]:

    _df['CDL3INSIDE'] = tb.CDL3INSIDE(open=_df['Open'], high=_df['High'], low=_df['Low'], close=_df['Close'])

    # ### CDL3LINESTRIKE - Three-Line Strike

    # In[184]:

    _df['CDL3LINESTRIKE'] = tb.CDL3LINESTRIKE(open=_df['Open'], high=_df['High'], low=_df['Low'],
                                              close=_df['Close'])

    # ### CDL3OUTSIDE - Three Outside Up/Down

    # In[185]:

    _df['CDL3OUTSIDE'] = tb.CDL3OUTSIDE(open=_df['Open'], high=_df['High'], low=_df['Low'], close=_df['Close'])

    # ### CDL3STARSINSOUTH - Three Stars In The South

    # In[186]:

    _df['CDL3STARSINSOUTH'] = tb.CDL3STARSINSOUTH(open=_df['Open'], high=_df['High'], low=_df['Low'],
                                                  close=_df['Close'])

    # ### CDL3WHITESOLDIERS - Three Advancing White Soldiers

    # In[187]:

    _df['CDL3WHITESOLDIERS'] = tb.CDL3WHITESOLDIERS(open=_df['Open'], high=_df['High'], low=_df['Low'],
                                                    close=_df['Close'])

    # de-fragmented frame, using `newframe = frame.copy()`
    _df = _df.copy(deep=True)

    # ### CDLABANDONEDBABY - Abandoned Baby

    # In[188]:

    _df['CDLABANDONEDBABY'] = tb.CDLABANDONEDBABY(open=_df['Open'], high=_df['High'], low=_df['Low'],
                                                  close=_df['Close'], penetration=0)

    # ### CDLADVANCEBLOCK - Advance Block

    # In[189]:

    _df['CDLADVANCEBLOCK'] = tb.CDLADVANCEBLOCK(open=_df['Open'], high=_df['High'], low=_df['Low'],
                                                close=_df['Close'])

    # ### CDLBELTHOLD - Belt-hold

    # In[190]:

    _df['CDLBELTHOLD'] = tb.CDLBELTHOLD(open=_df['Open'], high=_df['High'], low=_df['Low'], close=_df['Close'])

    # ### CDLBREAKAWAY - Breakaway

    # In[191]:

    _df['CDLBREAKAWAY'] = tb.CDLBREAKAWAY(open=_df['Open'], high=_df['High'], low=_df['Low'], close=_df['Close'])

    # ### CDLCLOSINGMARUBOZU - Closing Marubozu

    # In[192]:

    _df['CDLCLOSINGMARUBOZU'] = tb.CDLCLOSINGMARUBOZU(open=_df['Open'], high=_df['High'], low=_df['Low'],
                                                      close=_df['Close'])

    # ### CDLCONCEALBABYSWALL - Concealing Baby Swallow

    # In[193]:

    _df['CDLCONCEALBABYSWALL'] = tb.CDLCONCEALBABYSWALL(open=_df['Open'], high=_df['High'], low=_df['Low'],
                                                        close=_df['Close'])

    # ### CDLCOUNTERATTACK - Counterattack

    # In[194]:

    _df['CDLCOUNTERATTACK'] = tb.CDLCOUNTERATTACK(open=_df['Open'], high=_df['High'], low=_df['Low'],
                                                  close=_df['Close'])

    # ### CDLDARKCLOUDCOVER - Dark Cloud Cover

    # In[195]:

    _df['CDLDARKCLOUDCOVER'] = tb.CDLDARKCLOUDCOVER(open=_df['Open'], high=_df['High'], low=_df['Low'],
                                                    close=_df['Close'])

    # de-fragmented frame, using `newframe = frame.copy()`
    _df = _df.copy(deep=True)

    # ### CDLDOJI - Doji

    # In[196]:

    _df['CDLDOJI'] = tb.CDLDOJI(open=_df['Open'], high=_df['High'], low=_df['Low'], close=_df['Close'])

    # ### CDLDOJISTAR - Doji Star

    # In[197]:

    _df['CDLDOJISTAR'] = tb.CDLDOJISTAR(open=_df['Open'], high=_df['High'], low=_df['Low'], close=_df['Close'])

    # ### CDLDRAGONFLYDOJI - Dragonfly Doji

    # In[198]:

    _df['CDLDRAGONFLYDOJI'] = tb.CDLDRAGONFLYDOJI(open=_df['Open'], high=_df['High'], low=_df['Low'],
                                                  close=_df['Close'])

    # ### CDLENGULFING - Engulfing Pattern

    # In[199]:

    _df['CDLENGULFING'] = tb.CDLENGULFING(open=_df['Open'], high=_df['High'], low=_df['Low'], close=_df['Close'])

    # ### CDLEVENINGDOJISTAR - Evening Doji Star

    # In[200]:

    _df['CDLEVENINGDOJISTAR'] = tb.CDLEVENINGDOJISTAR(open=_df['Open'], high=_df['High'], low=_df['Low'],
                                                      close=_df['Close'])

    # ### CDLEVENINGSTAR - Evening Star

    # In[201]:

    _df['CDLEVENINGSTAR'] = tb.CDLEVENINGSTAR(open=_df['Open'], high=_df['High'], low=_df['Low'],
                                              close=_df['Close'])

    # ### CDLGAPSIDESIDEWHITE - Up/Down-gap side-by-side white lines

    # In[202]:

    _df['CDLGAPSIDESIDEWHITE'] = tb.CDLGAPSIDESIDEWHITE(open=_df['Open'], high=_df['High'], low=_df['Low'],
                                                        close=_df['Close'])

    # ### CDLGRAVESTONEDOJI - Gravestone Doji

    # In[203]:

    _df['CDLGRAVESTONEDOJI'] = tb.CDLGRAVESTONEDOJI(open=_df['Open'], high=_df['High'], low=_df['Low'],
                                                    close=_df['Close'])

    # de-fragmented frame, using `newframe = frame.copy()`
    _df = _df.copy(deep=True)

    # ### CDLHAMMER - Hammer

    # In[204]:

    _df['CDLHAMMER'] = tb.CDLHAMMER(open=_df['Open'], high=_df['High'], low=_df['Low'], close=_df['Close'])

    # ### CDLHANGINGMAN - Hanging Man

    # In[205]:

    _df['CDLHANGINGMAN'] = tb.CDLHANGINGMAN(open=_df['Open'], high=_df['High'], low=_df['Low'], close=_df['Close'])

    # ### CDLHARAMI - Harami Pattern

    # In[206]:

    _df['CDLHARAMI'] = tb.CDLHARAMI(open=_df['Open'], high=_df['High'], low=_df['Low'], close=_df['Close'])

    # ### CDLHARAMICROSS - Harami Cross Pattern

    # In[207]:

    _df['CDLHARAMICROSS'] = tb.CDLHARAMICROSS(open=_df['Open'], high=_df['High'], low=_df['Low'],
                                              close=_df['Close'])

    # ### CDLHIGHWAVE - High-Wave Candle

    # In[208]:

    _df['CDLHIGHWAVE'] = tb.CDLHIGHWAVE(open=_df['Open'], high=_df['High'], low=_df['Low'], close=_df['Close'])

    # ### CDLHIKKAKE - Hikkake Pattern

    # In[209]:

    _df['CDLHIKKAKE'] = tb.CDLHIKKAKE(open=_df['Open'], high=_df['High'], low=_df['Low'], close=_df['Close'])

    # ### CDLHIKKAKEMOD - Modified Hikkake Pattern

    # In[210]:

    _df['CDLHIKKAKEMOD'] = tb.CDLHIKKAKEMOD(open=_df['Open'], high=_df['High'], low=_df['Low'], close=_df['Close'])

    # ### CDLHOMINGPIGEON - Homing Pigeon

    # In[211]:

    _df['CDLHOMINGPIGEON'] = tb.CDLHOMINGPIGEON(open=_df['Open'], high=_df['High'], low=_df['Low'],
                                                close=_df['Close'])

    # ### CDLIDENTICAL3CROWS - Identical Three Crows

    # In[212]:

    _df['CDLIDENTICAL3CROWS'] = tb.CDLIDENTICAL3CROWS(open=_df['Open'], high=_df['High'], low=_df['Low'],
                                                      close=_df['Close'])

    # ### CDLINNECK - In-Neck Pattern

    # In[213]:

    _df['CDLINNECK'] = tb.CDLINNECK(open=_df['Open'], high=_df['High'], low=_df['Low'], close=_df['Close'])

    # ### CDLINVERTEDHAMMER - Inverted Hammer

    # In[214]:

    _df['CDLINVERTEDHAMMER'] = tb.CDLINVERTEDHAMMER(open=_df['Open'], high=_df['High'], low=_df['Low'],
                                                    close=_df['Close'])

    # de-fragmented frame, using `newframe = frame.copy()`
    _df = _df.copy(deep=True)

    # ### CDLKICKING - Kicking

    # In[215]:

    _df['CDLKICKING'] = tb.CDLKICKING(open=_df['Open'], high=_df['High'], low=_df['Low'], close=_df['Close'])

    # ### CDLKICKINGBYLENGTH - Kicking - bull/bear determined by the longer marubozu

    # In[216]:

    _df['CDLKICKINGBYLENGTH'] = tb.CDLKICKINGBYLENGTH(open=_df['Open'], high=_df['High'], low=_df['Low'],
                                                      close=_df['Close'])

    # ### CDLLADDERBOTTOM - Ladder Bottom

    # In[217]:

    _df['CDLLADDERBOTTOM'] = tb.CDLLADDERBOTTOM(open=_df['Open'], high=_df['High'], low=_df['Low'],
                                                close=_df['Close'])

    # ### CDLLONGLEGGEDDOJI - Long Legged Doji

    # In[218]:

    _df['CDLLONGLEGGEDDOJI'] = tb.CDLLONGLEGGEDDOJI(open=_df['Open'], high=_df['High'], low=_df['Low'],
                                                    close=_df['Close'])

    # ### CDLLONGLINE - Long Line Candle

    # In[219]:

    _df['CDLLONGLINE'] = tb.CDLLONGLINE(open=_df['Open'], high=_df['High'], low=_df['Low'], close=_df['Close'])

    # ### CDLMARUBOZU - Marubozu

    # In[220]:

    _df['CDLMARUBOZU'] = tb.CDLMARUBOZU(open=_df['Open'], high=_df['High'], low=_df['Low'], close=_df['Close'])

    # ### CDLMATCHINGLOW - Matching Low

    # In[221]:

    _df['CDLMATCHINGLOW'] = tb.CDLMATCHINGLOW(open=_df['Open'], high=_df['High'], low=_df['Low'],
                                              close=_df['Close'])

    # ### CDLMATHOLD - Mat Hold

    # In[222]:

    _df['CDLMATHOLD'] = tb.CDLMATHOLD(open=_df['Open'], high=_df['High'], low=_df['Low'], close=_df['Close'])

    # ### CDLMORNINGDOJISTAR - Morning Doji Star

    # In[223]:

    _df['CDLMORNINGDOJISTAR'] = tb.CDLMORNINGDOJISTAR(open=_df['Open'], high=_df['High'], low=_df['Low'],
                                                      close=_df['Close'])

    # de-fragmented frame, using `newframe = frame.copy()`
    _df = _df.copy(deep=True)

    # ### CDLMORNINGSTAR - Morning Star

    # In[224]:

    _df['CDLMORNINGSTAR'] = tb.CDLMORNINGSTAR(open=_df['Open'], high=_df['High'], low=_df['Low'],
                                              close=_df['Close'])

    # ### CDLONNECK - On-Neck Pattern

    # In[225]:

    _df['CDLONNECK'] = tb.CDLONNECK(open=_df['Open'], high=_df['High'], low=_df['Low'], close=_df['Close'])

    # ### CDLPIERCING - Piercing Pattern

    # In[226]:

    _df['CDLPIERCING'] = tb.CDLPIERCING(open=_df['Open'], high=_df['High'], low=_df['Low'], close=_df['Close'])

    # In[228]:

    # ### CDLRICKSHAWMAN - Rickshaw Man

    # In[229]:

    _df['CDLRICKSHAWMAN'] = tb.CDLRICKSHAWMAN(open=_df['Open'], high=_df['High'], low=_df['Low'],
                                              close=_df['Close'])

    # ### CDLRISEFALL3METHODS - Rising/Falling Three Methods

    # In[230]:

    _df['CDLRISEFALL3METHODS'] = tb.CDLRISEFALL3METHODS(open=_df['Open'], high=_df['High'], low=_df['Low'],
                                                        close=_df['Close'])

    # ### CDLSEPARATINGLINES - Separating Lines

    # In[231]:

    _df['CDLSEPARATINGLINES'] = tb.CDLSEPARATINGLINES(open=_df['Open'], high=_df['High'], low=_df['Low'],
                                                      close=_df['Close'])

    # ### CDLSHOOTINGSTAR - Shooting Star

    # In[232]:

    _df['CDLSHOOTINGSTAR'] = tb.CDLSHOOTINGSTAR(open=_df['Open'], high=_df['High'], low=_df['Low'],
                                                close=_df['Close'])

    # ### CDLSHORTLINE - Short Line Candle

    # In[233]:

    _df['CDLSHORTLINE'] = tb.CDLSHORTLINE(open=_df['Open'], high=_df['High'], low=_df['Low'], close=_df['Close'])

    # ### CDLSPINNINGTOP - Spinning Top

    # In[234]:

    _df['CDLSPINNINGTOP'] = tb.CDLSPINNINGTOP(open=_df['Open'], high=_df['High'], low=_df['Low'],
                                              close=_df['Close'])

    # ### CDLSTALLEDPATTERN - Stalled Pattern

    # In[235]:

    _df['CDLSTALLEDPATTERN'] = tb.CDLSTALLEDPATTERN(open=_df['Open'], high=_df['High'], low=_df['Low'],
                                                    close=_df['Close'])

    # de-fragmented frame, using `newframe = frame.copy()`
    _df = _df.copy(deep=True)

    # ### CDLSTICKSANDWICH - Stick Sandwich

    # In[236]:

    _df['CDLSTICKSANDWICH '] = tb.CDLSTICKSANDWICH(open=_df['Open'], high=_df['High'], low=_df['Low'],
                                                   close=_df['Close'])

    # ### CDLTAKURI - Takuri (Dragonfly Doji with very long lower shadow)

    # In[237]:

    _df['CDLTAKURI'] = tb.CDLTAKURI(open=_df['Open'], high=_df['High'], low=_df['Low'], close=_df['Close'])

    # ### CDLTASUKIGAP - Tasuki Gap

    # In[238]:

    _df['CDLTASUKIGAP'] = tb.CDLTASUKIGAP(open=_df['Open'], high=_df['High'], low=_df['Low'], close=_df['Close'])

    # ### CDLTHRUSTING - Thrusting Pattern

    # In[239]:

    _df['CDLTHRUSTING'] = tb.CDLTHRUSTING(open=_df['Open'], high=_df['High'], low=_df['Low'], close=_df['Close'])

    # ### CDLTRISTAR - Tristar Pattern

    # In[240]:

    _df['CDLTRISTAR'] = tb.CDLTRISTAR(open=_df['Open'], high=_df['High'], low=_df['Low'], close=_df['Close'])

    # ### CDLUNIQUE3RIVER - Unique 3 River

    # In[241]:

    _df['CDLUNIQUE3RIVER'] = tb.CDLUNIQUE3RIVER(open=_df['Open'], high=_df['High'], low=_df['Low'],
                                                close=_df['Close'])

    # ### CDLUPSIDEGAP2CROWS - Upside Gap Two Crows

    # In[242]:

    _df['CDLUPSIDEGAP2CROWS'] = tb.CDLUPSIDEGAP2CROWS(open=_df['Open'], high=_df['High'], low=_df['Low'],
                                                      close=_df['Close'])

    # ### CDLXSIDEGAP3METHODS - Upside/Downside Gap Three Methods

    # In[243]:

    _df['CDLUPSIDEGAP2CROWS'] = tb.CDLUPSIDEGAP2CROWS(open=_df['Open'], high=_df['High'], low=_df['Low'],
                                                      close=_df['Close'])

    # de-fragmented frame, using `newframe = frame.copy()`
    _df = _df.copy(deep=True)

    # # Statistic Functions

    # ### BETA - Beta

    # In[245]:

    for period in periods_5_10_20_50:
        _df[f'BETA_{period}'] = tb.BETA(_df['High'], _df['Low'], timeperiod=period)

    # ### CORREL - Pearson's Correlation Coefficient (r)

    # In[246]:

    for period in periods_5_10_20_30_50:
        _df[f'CORREL_{period}'] = tb.CORREL(_df['High'], _df['Low'], timeperiod=period)

    # ### LINEARREG - Linear Regression

    # In[247]:

    for period in periods_9_14_24:
        _df[f'LINEARREG_{period}'] = tb.LINEARREG(_df['Close'], timeperiod=period)

    # ### LINEARREG_ANGLE - Linear Regression Angle

    # In[248]:

    for period in periods_9_14_24:
        _df[f'LINEARREG_ANGLE_{period}'] = tb.LINEARREG_ANGLE(_df['Close'], timeperiod=period)

    # ### LINEARREG_INTERCEPT - Linear Regression Intercept

    # In[249]:

    for period in periods_9_14_24:
        _df[f'LINEARREG_INTERCEPT_{period}'] = tb.LINEARREG_INTERCEPT(_df['Close'], timeperiod=period)

    # ### LINEARREG_SLOPE - Linear Regression Slope

    # In[250]:

    for period in periods_9_14_24:
        _df[f'LINEARREG_SLOPE_{period}'] = tb.LINEARREG_SLOPE(_df['Close'], timeperiod=period)

    # ### STDDEV - Standard Deviation

    # In[251]:

    for period in periods_9_14_24:
        _df[f'STDDEV_{period}'] = tb.STDDEV(_df['Close'], timeperiod=period)

    # ### TSF - Time Series Forecast

    # In[252]:

    for period in periods_9_14_24:
        _df[f'TSF_{period}'] = tb.TSF(_df['Close'], timeperiod=period)

    # ### VAR - Variance

    # In[253]:

    for period in periods_9_14_24:
        _df[f'VAR_{period}'] = tb.VAR(_df['Close'], timeperiod=period, nbdev=1)

    return _df


def deviation_proportion(base_price: float, price: float) -> float:
    devit = (price - base_price) / base_price
    return devit.values[0]


def fetch_btc_n_data(n: int, ref_time_type='start_time', end_time_in_sec=0):
    error_in_fetch_btc_n_data = False
    now_timestamp = int(datetime.datetime.now().timestamp())
    print(f">>> {datetime.datetime.fromtimestamp(now_timestamp)}: Fetching new BTC data from Binance...")

    try:
        if ref_time_type == 'start_time':
            _list = get_klines(symbol=config.get('SYMBOL'),
                               ref_time_type=ref_time_type,  # start_time: from now and we go in the past
                               ref_time_in_sec=now_timestamp,
                               interval_in_minute=config.get('INTERVAL_IN_MINUTES'),
                               expected_total_number_rows=n,
                               number_rows_per_request=n)
        else:

            # ref_time_type == 'end_time':
            # Get the number of minute from last_open_time to now
            minute_range = (now_timestamp - end_time_in_sec) / 60
            # Get the number of expected rows
            expected_rows = int(minute_range / config.get('INTERVAL_IN_MINUTES'))
            n = expected_rows if expected_rows < n else n
            _list = get_klines(symbol=config.get('SYMBOL'),
                               ref_time_type=ref_time_type,  # end_time: from the past and we move forward
                               ref_time_in_sec=end_time_in_sec,
                               interval_in_minute=config.get('INTERVAL_IN_MINUTES'),
                               expected_total_number_rows=expected_rows,
                               number_rows_per_request=n)

            # print("_list")
            # print(_list)

    except Exception as e:
        error_in_fetch_btc_n_data = True
        print("ERROR: ", e)
        _list = []

    if not error_in_fetch_btc_n_data:
        # ERROR: In fetching new data
        df = pd.DataFrame(_list)

        df.columns = config.get('DATAFRAME_COLUMN_NAMES_LIST')
        df.drop('Can_be_ignored', axis=1, inplace=True)

        cols_types = df.dtypes
        for _col, _type in zip(cols_types.keys(), cols_types.values):
            if _type == "object":
                df[_col] = df[_col].astype(float)

        # Add indicators and return
        return add_indicators(df)


def get_klines(symbol: str, ref_time_type: str, ref_time_in_sec: int,
               interval_in_minute: int, expected_total_number_rows: int,
               number_rows_per_request: int) -> list:
    """
    :param symbol:
    :param ref_time_type:
    :param ref_time_in_sec:
    :param interval_in_minute:
    :param expected_total_number_rows:
    :param number_rows_per_request:
    :return:
    """

    if ref_time_type not in ['start_time', 'end_time']:
        # add_to_log(obj=dict(type="Error in get_klines()"))
        print(f"ERROR: ref_time_in_sec_type must 'start_time' or 'end_time', {ref_time_type} was given")
        return list()

    number_of_iteration = int(expected_total_number_rows / number_rows_per_request)

    total_diff_time_in_millisecond = \
        (interval_in_minute * 60 * 1000 * number_of_iteration) * number_rows_per_request
    diff_time_in_millisecond_per_request = int(total_diff_time_in_millisecond / number_of_iteration)

    if ref_time_type == "start_time":
        diff_time_in_millisecond_per_request *= -1

    start_time_end_time = []
    ref_time_in_millisecond = ref_time_in_sec * 1000

    for i in range(number_of_iteration):
        second_time = ref_time_in_millisecond + diff_time_in_millisecond_per_request

        start_time, end_time = min(second_time, ref_time_in_millisecond), max(second_time, ref_time_in_millisecond)

        start_time_end_time.append((start_time, end_time))

        ref_time_in_millisecond += diff_time_in_millisecond_per_request

    if ref_time_type == "start_time":
        start_time_end_time.reverse()  # Reverse to lowest to greatest

    # print(f"start_time_end_time: {start_time_end_time}")

    _list = []
    # Start request
    params = None
    for start_time, end_time in start_time_end_time:
        # print(f"start_time, end_time: {start_time, end_time}")

        params = {
            'symbol': f'{symbol}',
            'interval': f'{interval_in_minute}m',
            'limit': number_rows_per_request,
            'startTime': int(start_time),
            'endTime': int(end_time)
        }
        # print(f"params: {params}")
        # Send request to tesnet server
        data_binance = get_testnet_klines(params)

        if len(data_binance):
            _list += data_binance

    return _list


def get_testnet_klines(params: dict):
    headers = {
        'X-MBX-APIKEY': config.get('API_Key'),
        'Content-Type': 'application/json',
        # 'application/x-www-form-urlencoded' 'application/x-www-form-urlencoded'
        'charset': 'UTF-8'
    }
    base_url = "https://testnet.binance.vision/api/v3/klines"
    q_body = urllib.parse.urlencode(params)

    url = base_url + "?" + q_body
    rsp = requests.get(url, headers=headers).json()
    # print("rsp: ", rsp)
    return rsp


# ---------------------- Data base functions
def get_mysql_user_connection():
    conn = mysql.connector.connect(
        host=config.get('MYSQL_HOST'),
        database=config.get('MYSQL_DATABASE_NAME'),
        user="testnet",
        password="Testnet00"
    )

    return conn


def init_database(df: pd.DataFrame):
    print("--------- Creating table employee")
    # Create table history_indicators
    # Create a cursor object
    cnx = get_mysql_user_connection()

    # Create a cursor object to execute SQL queries
    cursor = cnx.cursor()
    # Generate the SQL code for creating the table structure
    table_name = config.get('MAIN_TABLE_NAME')
    # Get the data types of each column
    data_types = df.dtypes
    columns_sql = ', '.join(f'{column} {dtype}' for column, dtype in data_types.items())
    sql_code = f'CREATE TABLE {table_name} ({columns_sql});'
    sql_code = sql_code.replace("int32", "INT")
    sql_code = sql_code.replace("int64", "BIGINT")
    sql_code = sql_code.replace("float64", "DOUBLE")
    print(sql_code)
    cursor.execute(sql_code)

    # Commit the changes to the database
    cnx.commit()

    cursor.close()


def table_exist(table_name):
    conn = get_mysql_user_connection()

    cursor = conn.cursor()
    query = f"SHOW TABLES LIKE '{table_name}'"
    cursor.execute(query)
    result = cursor.fetchone()
    cursor.close()
    conn.close()

    if result:
        return True
    else:
        return False


def generate_insert_sql(table_name: str, dataframe: pd.DataFrame):
    columns = dataframe.columns.tolist()
    values = []

    for _, row in dataframe.iterrows():
        row_values = row.values.tolist()
        formatted_values = []

        for value in row_values:
            if pd.isnull(value):
                formatted_values.append("NULL")
            elif isinstance(value, str):
                formatted_values.append(f"'{value}'")
            else:
                formatted_values.append(str(value))

        values.append("(" + ", ".join(formatted_values) + ")")

    insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES\n"
    insert_sql += ",\n".join(values) + ";"

    return insert_sql


def get_most_recent_open_time():
    last_inserted_open_time = None
    sql_query = F"""
    SELECT Open_time
    FROM {config.get('MAIN_TABLE_NAME')}
    ORDER BY Open_time DESC LIMIT 1;
    """
    print(sql_query)
    result = execute_sql_query(sql_query=sql_query, execution_type="fetch")

    # Extract the value of id_column from the result
    if result:
        last_inserted_open_time = result[0]

    return last_inserted_open_time


def execute_sql_query(sql_query: str, execution_type: str):
    # Create a cursor object
    cnx = get_mysql_user_connection()
    # Create a cursor object to execute SQL queries
    cursor = cnx.cursor()

    if execution_type not in ["execute", "fetch"]:
        # Handle invalid mode
        print("Invalid mode. Please choose 'execute' or 'fetch'.")
    else:
        result = None
        # Perform execute operation
        # print("Executing...")
        result = cursor.execute(sql_query)

        if execution_type == "fetch":
            # Perform fetch operation
            # print("Fetching...")
            result = cursor.fetchone()
        else:
            # Commit the changes to the database
            cnx.commit()

        cursor.close()
        cnx.close()

        return result
