import os

config = dict(
    API_Key=os.getenv('API_Key'),
    API_Secret=os.getenv('API_Secret'),
    MYSQL_USER=os.getenv('MYSQL_USER'),
    MYSQL_PASSWORD=os.getenv('MYSQL_PASSWORD'),
    MYSQL_HOST='localhost',
    MYSQL_PORT=3306,
    MYSQL_DATABASE_NAME='testnet_base',
    MAIN_TABLE_NAME='indicators_history',
    SYMBOL="BTCUSDT",
    INTERVAL_IN_MINUTES=5,
    FETCHING_INTERVAL_SECOND=70,  # 6*60 = Every 6 minutes
    NUMBER_ROWS_PER_REQUEST=300,  # 20 = the 20 last entries from now
    CANDLES_TIME_INTERVAL_MINUTE=1,  # 5 = 5 minutes candles, But not use in this ETL project as many others
    MAXIMUM_TRIES_PER_ORDER=3,
    DECISION_CLASSES=['Hold', 'Buy', 'Sell'],
    DATAFRAME_COLUMN_NAMES_LIST=['Open_time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close_time',
                                 'Quote_asset_volume', 'Number_of_trades', 'Taker_buy_base_asset_volume',
                                 'Taker_buy_quote_asset_volume', 'Can_be_ignored'],
    MAX_LIMIT_OF_BUY_PER_ORDER_IN_USD=200,  # In USD
    MODELS_LIST=[
        'DTree_model_pipe_1__prec_0969.pkl',
        'DTree_model_pipe_PAC_1__prec_0792.pkl',
        'RandomForest_model_pipe_1__prec_0916.pkl',
        'DTree_model_pipe_1__intervalInMinute_5__scopeLenght_10__var_0.025__prec_0919__auc_0958.pkl',
        'DTree_model_pipe__intervalInMinute_5__scopeLenght_30__sub_div_deviat_step_5__var_0.025__prec_0901__auc_0946.pkl',
        'DTree_model_pipe_2_intervalInMinute_5__scopeLenght_30__sub_div_deviat_step_5__var_0.023__prec_0.887__auc_0.951.pkl',
        'DTree_model_pipe_3_intervalInMinute_5__scopeLenght_20__sub_div_deviat_step_1__var_0.01__prec_0.827__auc_0.922.pkl',
        'DTree_model_pipe_4_intervalInMinute_5__scopeLenght_15__sub_div_deviat_step_1__var_0.009__prec_0.828__auc_0.923.pkl'
    ],

    BINANCE_BASE_URL_API="https://api.binance.com",
    API_TESTNET_URL="https://testnet.binance.vision/api",
    BINANCE_BASE_TEST_API="https://testnet.binancefuture.com",
    TEST_MAKE_ORDER_END_POINT="fapi/v1/order",
    BINANCE_HEADERS={
        'X-MBX-APIKEY': os.getenv('API_Key'),
        'Content-Type': 'application/json'
    },

    SELL_CONFIRM_PROBABILITY_THRESHOLD=0.8,  # if None then probabilities are ignored
    BUY_CONFIRM_PROBABILITY_THRESHOLD=0.8,  # if None then probabilities are ignored
    MINIMUM_VARIATION_PROPORTION_TO_SHORT_POSITION=0.02,  # if prediction is "Hold" then this is checked

    TRADES_LOGS_FILE="trades_logs.json",
    LAST_BUY_POSITION_FILE="last_buy_position.JSON",
    LOGS_FILE_NAME="logs.json",
    LOGS_DIR="logs",
)


def get_config() -> dict:
    return config
