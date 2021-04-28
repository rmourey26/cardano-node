module Cardano.Logger.Handlers.Metrics.Run
  ( runMetricsHandler
  ) where

import           Cardano.Logger.Configuration
import           Cardano.Logger.Types (AcceptedItems)

runMetricsHandler
  :: LoggerConfig
  -> AcceptedItems
  -> IO ()
runMetricsHandler config acceptedItems = return ()
