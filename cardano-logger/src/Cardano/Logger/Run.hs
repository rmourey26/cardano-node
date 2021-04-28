{-# LANGUAGE RecordWildCards #-}

-- | This top-level module will be used by the 'cardano-logger' app.
module Cardano.Logger.Run
  ( runCardanoLogger
  ) where

import           Control.Concurrent.Async (concurrently_)

import           Cardano.Logger.Acceptors (runAcceptors)
import           Cardano.Logger.CLI (LoggerParams (..))
import           Cardano.Logger.Configuration (readLoggerConfig)
import           Cardano.Logger.Handlers (runHandlers)
import           Cardano.Logger.Types (initAcceptedItems)

runCardanoLogger
  :: LoggerParams
  -> IO ()
runCardanoLogger LoggerParams{..} = do
  config <- readLoggerConfig loggerConfig
  acceptedItems <- initAcceptedItems
  -- Run two main threads:
  -- 1. For all acceptors: they ask 'LogObject's and metrics from the node
  --    and collect them in 'acceptedItems'.
  -- 2. For all handlers: they take items from 'acceptedItems' and do something
  --    with them (write to log files and return by web-request via EKG API).
  concurrently_ (runAcceptors config acceptedItems)
                (runHandlers  config acceptedItems)
