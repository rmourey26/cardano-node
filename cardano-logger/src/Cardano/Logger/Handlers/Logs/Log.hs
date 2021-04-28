{-# LANGUAGE OverloadedStrings #-}

module Cardano.Logger.Handlers.Logs.Log
  ( logPrefix
  , logExtension
  , symLinkName
  , isItSymLink
  , isItLog
  , createLogAndSymLink
  , createLogAndUpdateSymLink
  , getTimeStampFromLog
  ) where

import qualified Data.ByteString.Lazy as LBS
import           Data.Char (isDigit)
import           Data.Time (UTCTime, getCurrentTime)
import           Data.Time.Format (defaultTimeLocale, formatTime, parseTimeM)
import qualified Data.Text as T
import           System.Directory (createFileLink, pathIsSymbolicLink, renamePath)
import           System.FilePath ((<.>), takeBaseName, takeExtension)

import           Cardano.Logger.Configuration

logPrefix :: String
logPrefix = "node-"

logExtension :: LogFormat -> String
logExtension AsText = ".log"
logExtension AsJSON = ".json"

symLinkName :: LogFormat -> FilePath
symLinkName format = "node" <.> logExtension format

symLinkNameTmp :: LogFormat -> FilePath
symLinkNameTmp format = symLinkName format <.> "tmp"

isItSymLink
  :: FilePath
  -> LogFormat
  -> IO Bool
isItSymLink fileName format =
  if fileName == symLinkName format
    then pathIsSymbolicLink fileName
    else return False

isItLog :: LogFormat -> FilePath -> Bool
isItLog format fileName = hasProperPrefix && hasTimestamp && hasProperExt
 where
  hasProperPrefix = T.pack logPrefix `T.isPrefixOf` T.pack fileName
  hasTimestamp    = T.length maybeTimestamp == 14 && T.all isDigit maybeTimestamp
  maybeTimestamp  = T.drop (length logPrefix) . T.pack . takeBaseName $ fileName
  hasProperExt    = takeExtension fileName == logExtension format

-- | Create a new log file and symlink to it, from scratch.
createLogAndSymLink :: LogFormat -> IO ()
createLogAndSymLink format =
  createLog format >>= flip createFileLink (symLinkName format)

-- | Create a new log file and move existing symlink
-- from the old log file to the new one.
createLogAndUpdateSymLink :: LogFormat -> IO ()
createLogAndUpdateSymLink format = do
  newLog <- createLog format
  let tmpSymLink  = symLinkNameTmp format
      realSymLink = symLinkName format
  createFileLink newLog tmpSymLink
  renamePath tmpSymLink realSymLink -- Atomic operation, uses POSIX.rename.

createLog :: LogFormat -> IO FilePath
createLog format = do
  ts <- formatTime defaultTimeLocale timeStampFormat <$> getCurrentTime
  let logName = logPrefix <> ts <.> logExtension format
  LBS.writeFile logName LBS.empty
  return logName

-- | This function is applied to the log we already checked,
-- so we definitely know it contains timestamp.
getTimeStampFromLog :: FilePath -> Maybe UTCTime
getTimeStampFromLog logName =
  parseTimeM True defaultTimeLocale timeStampFormat timeStamp
 where
  timeStamp = drop (length logPrefix) . takeBaseName $ logName

timeStampFormat :: String
timeStampFormat = "%Y%m%d%H%M%S"
