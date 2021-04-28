module Cardano.Logger.Handlers
  ( runHandlers
  ) where

import           Control.Concurrent.Async (concurrently_)

import           Cardano.Logger.Configuration
import           Cardano.Logger.Types (AcceptedItems)
import           Cardano.Logger.Handlers.Logs.Run (runLogsHandler) 
import           Cardano.Logger.Handlers.Metrics.Run (runMetricsHandler)

runHandlers
  :: LoggerConfig
  -> AcceptedItems
  -> IO ()
runHandlers config acceptedItems =
  concurrently_ (runLogsHandler    config acceptedItems)
                (runMetricsHandler config acceptedItems)
