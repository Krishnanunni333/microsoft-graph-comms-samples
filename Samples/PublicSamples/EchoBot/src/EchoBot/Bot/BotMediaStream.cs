// ***********************************************************************
// Assembly         : EchoBot.Services
// Author           : JasonTheDeveloper (Modified for Redis)
// Created          : 09-07-2020
//
// Last Modified By : Gemini
// Last Modified On : 2025-04-15
// ***********************************************************************
// <copyright file="BotMediaStream.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.
// </copyright>
// <summary>The bot media stream modified to play audio from Redis.</summary>
// ***********************************************************************-
using EchoBot.Util; // Assuming AppSettings and Utilities are here
using EchoBot.Media;
using Microsoft.Graph.Communications.Calls;
using Microsoft.Graph.Communications.Calls.Media;
using Microsoft.Graph.Communications.Common;
using Microsoft.Graph.Communications.Common.Telemetry;
using Microsoft.Skype.Bots.Media; // Required for AudioVideoFramePlayer, AudioMediaBuffer etc.
using StackExchange.Redis;
using System; // Added for Exception, TimeSpan, DateTime, GC, etc.
using System.Collections.Generic; // Added for List<>
using System.Linq; // Added for .Any()
using System.Threading; // Added for Interlocked
using System.Threading.Tasks; // Added for Task, TaskCompletionSource

// Ensure you have a using directive for your logger implementation (e.g., Microsoft.Extensions.Logging)
using Microsoft.Extensions.Logging;
using Microsoft.Skype.Internal.Media.Services.Common; // Example if using Microsoft.Extensions.Logging

namespace EchoBot.Bot
{
    /// <summary>
    /// Class responsible for streaming audio received from a Redis channel.
    /// </summary>
    public class BotMediaStream : ObjectRootDisposable
    {
        /// <summary>
        /// The participants
        /// </summary>
        internal List<IParticipant> participants;

        /// <summary>
        /// The audio socket
        /// </summary>
        private readonly IAudioSocket _audioSocket;
        /// <summary>
        /// The media stream
        /// </summary>
        private readonly AppSettings _settings;
        private readonly ILogger _logger;
        private AudioVideoFramePlayer _audioVideoFramePlayer;
        private readonly TaskCompletionSource<bool> _audioSendStatusActive;
        private readonly TaskCompletionSource<bool> _startAVPlayerCompleted;
        private AudioVideoFramePlayerSettings _audioVideoFramePlayerSettings;
        private int _shutdown;

        // --- Redis Members ---
        private ConnectionMultiplexer _redisConnection;
        private ISubscriber _redisSubscriber;
        private const string RedisChannelName = "audio_stream"; // Must match Python script's channel
        private long _lastRedisAudioTimestamp = -1; // For generating timestamps
        private const long TicksPerMs = TimeSpan.TicksPerMillisecond;
        private const int AudioChunkDurationMs = 20; // Must match Python script's chunk duration

        /// <summary>
        /// Initializes a new instance of the <see cref="BotMediaStream" /> class.
        /// Connects to Redis and prepares to play audio from the specified channel.
        /// </summary>
        public BotMediaStream(
            ILocalMediaSession mediaSession,
            string callId, // Although callId is passed, it's not explicitly used in this simplified version
            IGraphLogger graphLogger,
            ILogger logger, // Ensure this logger is compatible with LogInformation, LogError etc.
            AppSettings settings
        )
            : base(graphLogger) // Pass the GraphLogger to the base class
        {
            ArgumentVerifier.ThrowOnNullArgument(mediaSession, nameof(mediaSession));
            ArgumentVerifier.ThrowOnNullArgument(logger, nameof(logger));
            ArgumentVerifier.ThrowOnNullArgument(settings, nameof(settings));

            _settings = settings;
            _logger = logger;
            _audioSendStatusActive = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            _startAVPlayerCompleted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            // Subscribe to the audio socket. Required for sending audio.
            this._audioSocket = mediaSession.AudioSocket;
            if (this._audioSocket == null)
            {
                _logger.LogError("Media session does not have an audio socket.");
                throw new InvalidOperationException("A mediaSession needs to have at least an audioSocket to send audio.");
            }

            // Start the process of creating the AudioVideo frame player
            // We need the player to be ready before we can enqueue buffers
            var ignoreTask = this.StartAudioVideoFramePlayerAsync().ForgetAndLogExceptionAsync(this.GraphLogger, "Failed to start the AV player");

            // Subscribe ONLY to the AudioSendStatusChanged event.
            // We need this to know when the media platform is ready to receive audio buffers.
            this._audioSocket.AudioSendStatusChanged += OnAudioSendStatusChanged;

            // *** DO NOT SUBSCRIBE TO AudioMediaReceived ***
            // This prevents the bot from receiving (and looping back) the caller's audio.
            // this._audioSocket.AudioMediaReceived += this.OnAudioMediaReceived; // <-- REMOVED / COMMENTED OUT

            // Initialize connection to Redis
            InitializeRedis();
        }

        /// <summary>
        /// Cleans up resources, unsubscribing from Redis and shutting down the player.
        /// </summary>
        public async Task ShutdownAsync()
        {
            // Ensure shutdown logic runs only once
            if (Interlocked.CompareExchange(ref this._shutdown, 1, 0) == 1)
            {
                return;
            }

            _logger.LogInformation("[BotMediaStream] Initiating shutdown...");

            // --- Redis Cleanup ---
            if (_redisSubscriber != null)
            {
                try
                {
                    _logger.LogInformation($"[BotMediaStream] Unsubscribing from Redis channel: {RedisChannelName}");
                    // Use ConfigureAwait(false) to avoid deadlocks in certain contexts
                    await _redisSubscriber.UnsubscribeAsync(RedisChannelName).ConfigureAwait(false);
                    _redisSubscriber = null; // Release the reference
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "[BotMediaStream] Error during Redis unsubscribe.");
                }
            }
            if (_redisConnection != null)
            {
                try
                {
                    _logger.LogInformation("[BotMediaStream] Closing Redis connection.");
                    await _redisConnection.CloseAsync().ConfigureAwait(false);
                    _redisConnection.Dispose(); // Dispose the connection object
                    _redisConnection = null; // Release the reference
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "[BotMediaStream] Error during Redis connection close/dispose.");
                }
            }
            // --- End Redis Cleanup ---

            // Wait for the player to have been created before trying to shut it down
            await this._startAVPlayerCompleted.Task.ConfigureAwait(false);

            // Unsubscribe from the socket event
            if (this._audioSocket != null)
            {
                this._audioSocket.AudioSendStatusChanged -= this.OnAudioSendStatusChanged;
            }

            // Shut down the media player
            if (this._audioVideoFramePlayer != null)
            {
                _logger.LogInformation("[BotMediaStream] Shutting down AudioVideoFramePlayer.");
                await this._audioVideoFramePlayer.ShutdownAsync().ConfigureAwait(false);
                this._audioVideoFramePlayer = null; // Release reference
            }

            _logger.LogInformation("[BotMediaStream] Shutdown completed.");

            // Dispose base class resources
            base.Dispose(true); // Assuming true indicates disposing managed resources
            GC.SuppressFinalize(this); // Prevent finalizer from running
        }

        /// <summary>
        /// Initializes the AudioVideoFramePlayer used to send audio buffers.
        /// </summary>
        private async Task StartAudioVideoFramePlayerAsync()
        {
            try
            {
                _logger.LogInformation("[BotMediaStream] Creating the audio/video frame player.");
                // Settings for the player - primarily audio settings needed here.
                // AudioSettings(20) likely refers to 20ms buffer duration.
                this._audioVideoFramePlayerSettings = new AudioVideoFramePlayerSettings(new AudioSettings(20), new VideoSettings(), 1000);

                // Create the player instance, linking it to the audio socket.
                // We pass null for the video socket as we're only dealing with audio.
                this._audioVideoFramePlayer = new AudioVideoFramePlayer(
                    (AudioSocket)_audioSocket, // Cast may be necessary depending on exact type
                    null, // No video socket
                    this._audioVideoFramePlayerSettings);

                _logger.LogInformation("[BotMediaStream] Audio/video frame player created successfully.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[BotMediaStream] Failed to create the AudioVideoFramePlayer.");
                // Signal completion even on failure to prevent deadlocks waiting for it
                this._startAVPlayerCompleted.TrySetException(ex);
                return; // Exit if creation failed
            }
            finally
            {
                // Signal that the player creation process (success or fail) is complete.
                this._startAVPlayerCompleted.TrySetResult(true);
            }
        }

        /// <summary>
        /// Handles changes in the audio send status from the media platform.
        /// We wait for the status to become 'Active' before sending audio.
        /// </summary>
        private void OnAudioSendStatusChanged(object? sender, AudioSendStatusChangedEventArgs e)
        {
            _logger.LogInformation($"[BotMediaStream] AudioSendStatus changed to {e.MediaSendStatus}");

            // Once the status is Active, signal that we can start sending audio buffers.
            if (e.MediaSendStatus == MediaSendStatus.Active)
            {
                this._audioSendStatusActive.TrySetResult(true);
            }
            // Optional: Handle other statuses like Inactive if needed
            else if (e.MediaSendStatus == MediaSendStatus.Inactive)
            {
                // If it becomes inactive, reset the completion source *if* you want to wait again
                // _audioSendStatusActive = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                _logger.LogWarning("[BotMediaStream] AudioSendStatus is Inactive.");
            }
        }

        /// <summary>
        /// Establishes the connection to the Redis server.
        /// </summary>
        private void InitializeRedis()
        {
            try
            {
                // Get connection string from settings, fallback to localhost default
                string redisConnectionString = _settings.RedisConnectionString ?? "localhost:6379,abortConnect=False,connectTimeout=30000,responseTimeout=30000";
                _logger.LogInformation($"[BotMediaStream] Connecting to Redis: {redisConnectionString}");

                var options = ConfigurationOptions.Parse(redisConnectionString);
                options.AbortOnConnectFail = false; // Don't throw immediately if connection fails
                //options.AsyncTimeout = 5000;       // Example: 5 second timeout for async operations
                //options.ConnectTimeout = 5000;     // Example: 5 second timeout for initial connection

                _redisConnection = ConnectionMultiplexer.Connect(options);

                // Log connection events for debugging
                _redisConnection.ConnectionFailed += (sender, args) => {
                    _logger.LogError($"[BotMediaStream] Redis connection failed: {args.FailureType}, Endpoint: {args.EndPoint}, Exception: {args.Exception?.Message}");
                };
                _redisConnection.ConnectionRestored += (sender, args) => {
                    _logger.LogInformation($"[BotMediaStream] Redis connection restored: {args.FailureType}, Endpoint: {args.EndPoint}");
                    // Re-subscribe when connection is restored
                    SubscribeToAudioChannel();
                };
                _redisConnection.ErrorMessage += (sender, args) => {
                    _logger.LogError($"[BotMediaStream] Redis error message: {args.Message}");
                };

                if (_redisConnection.IsConnected)
                {
                    _logger.LogInformation("[BotMediaStream] Successfully connected to Redis.");
                    SubscribeToAudioChannel(); // Subscribe after successful connection
                }
                else
                {
                    _logger.LogWarning("[BotMediaStream] Initial Redis connection failed. Will attempt to reconnect automatically.");
                    // StackExchange.Redis handles reconnection automatically
                }
            }
            catch (RedisConnectionException ex)
            {
                _logger.LogError(ex, "[BotMediaStream] Failed to establish initial Redis connection.");
            }
            catch (Exception ex) // Catch broader exceptions during init
            {
                _logger.LogError(ex, "[BotMediaStream] An unexpected error occurred during Redis initialization.");
            }
        }

        /// <summary>
        /// Subscribes to the specified Redis channel to receive audio chunks.
        /// </summary>
        private void SubscribeToAudioChannel()
        {
            if (_redisConnection == null || !_redisConnection.IsConnected)
            {
                _logger.LogWarning("[BotMediaStream] Cannot subscribe, Redis connection is not available.");
                return;
            }

            // Avoid re-subscribing if already subscribed
            if (_redisSubscriber != null && _redisSubscriber.IsConnected(RedisChannelName))
            {
                _logger.LogInformation($"[BotMediaStream] Already subscribed to Redis channel: {RedisChannelName}");
                return;
            }

            try
            {
                _redisSubscriber = _redisConnection.GetSubscriber();

                // Subscribe to the channel, directing messages to OnRedisAudioReceived
                // Use SubscribeAsync for consistency, although fire-and-forget is often acceptable here.
                _redisSubscriber.Subscribe(RedisChannelName, OnRedisAudioReceived); // Simple sync-over-async subscribe

                _logger.LogInformation($"[BotMediaStream] Subscribed to Redis channel: {RedisChannelName}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"[BotMediaStream] Failed to subscribe to Redis channel {RedisChannelName}.");
            }
        }

        /// <summary>
        /// Handles audio chunk messages received from the Redis channel.
        /// Creates AudioMediaBuffers and enqueues them for playback.
        /// </summary>
        private async void OnRedisAudioReceived(RedisChannel channel, RedisValue message)
        {
            // If shutdown has started, ignore incoming messages
            if (this._shutdown == 1) return;

            try
            {
                if (!message.HasValue || message.IsNullOrEmpty)
                {
                    _logger.LogWarning("[BotMediaStream] Received empty message from Redis.");
                    return;
                }

                byte[] audioChunk = (byte[])message; // Cast RedisValue to byte array

                // Check for End-of-Stream (EOS) marker sent by the Python script
                if (audioChunk.Length <= 3 && System.Text.Encoding.UTF8.GetString(audioChunk) == "EOS")
                {
                    _logger.LogInformation("[BotMediaStream] Received EOS marker from Redis. Playback complete.");
                    // Optional: Add logic here if you need to signal completion elsewhere
                    return;
                }

                // --- Wait until player and send status are ready ---
                // Ensure the player object is created AND the media platform is ready to send.
                // Use timeouts to prevent waiting forever if something goes wrong.
                var playerReadyTask = this._startAVPlayerCompleted.Task;
                var sendReadyTask = this._audioSendStatusActive.Task;

                // Wait for both tasks with a timeout (e.g., 5 seconds)
                var completedTask = await Task.WhenAny(Task.WhenAll(playerReadyTask, sendReadyTask), Task.Delay(5000)).ConfigureAwait(false);

                if (completedTask is Task<Task> || !playerReadyTask.IsCompletedSuccessfully || !sendReadyTask.IsCompletedSuccessfully)
                {
                    _logger.LogWarning("[BotMediaStream] Timed out or failed waiting for AV player/send status to be active. Skipping Redis chunk.");
                    return;
                }


                // Double-check player instance in case shutdown happened between checks
                if (this._audioVideoFramePlayer == null || this._shutdown == 1)
                {
                    _logger.LogWarning("[BotMediaStream] AudioVideoFramePlayer not ready or shutdown initiated after wait. Skipping Redis audio chunk.");
                    return;
                }
                // --- End Readiness Check ---


                _logger.LogTrace($"[BotMediaStream] Received Redis Audio Chunk: {audioChunk.Length} bytes on channel {channel}");

                // --- Create AudioMediaBuffer(s) using the Utility method ---
                long currentTimestamp = GenerateTimestamp();

                // *** Use the utility method from your project to create the buffer list ***
                // Make sure 'Util.Utilities' is the correct namespace/class containing this method.
                List<AudioMediaBuffer> buffersToEnqueue = Util.Utilities.CreateAudioMediaBuffers(audioChunk, currentTimestamp, _logger);

                if (buffersToEnqueue == null || !buffersToEnqueue.Any())
                {
                    _logger.LogWarning("[BotMediaStream] CreateAudioMediaBuffers returned no buffers for the received chunk.");
                    return; // Don't try to enqueue nothing
                }

                // --- Enqueue the buffer(s) for playback ---
                await this._audioVideoFramePlayer.EnqueueBuffersAsync(buffersToEnqueue, null).ConfigureAwait(false); // Pass null or empty list for video
                _logger.LogTrace($"[BotMediaStream] Enqueued {buffersToEnqueue.Count} Redis audio buffer(s). Timestamp: {currentTimestamp}");

                // NOTE on Disposal: Assume CreateAudioMediaBuffers provides buffers that
                // are managed/disposed by the AudioVideoFramePlayer after enqueuing.
                // If explicit disposal is needed, it would happen *after* the data is sent,
                // which EnqueueBuffersAsync handles asynchronously. Manual disposal here is risky.
            }
            catch (ObjectDisposedException)
            {
                // This is expected if messages arrive during or after shutdown/disposal
                _logger.LogWarning("[BotMediaStream] Attempted to process Redis audio after object disposal.");
            }
            catch (Exception ex) // Catch any other unexpected errors
            {
                _logger.LogError(ex, "[BotMediaStream] Error processing audio chunk received from Redis.");
            }
        }

        /// <summary>
        /// Generates synchronized timestamps for outgoing audio buffers based on elapsed time.
        /// </summary>
        /// <returns>A timestamp in Ticks.</returns>
        private long GenerateTimestamp()
        {
            long currentTimestamp;
            long nowTicks = DateTime.UtcNow.Ticks;

            if (_lastRedisAudioTimestamp < 0)
            {
                // First buffer uses current time
                currentTimestamp = nowTicks;
            }
            else
            {
                // Subsequent buffers add the chunk duration to the last timestamp
                currentTimestamp = _lastRedisAudioTimestamp + (AudioChunkDurationMs * TicksPerMs);

                // Basic drift correction: If the calculated timestamp is significantly
                // behind the actual current time, reset to 'now' to avoid falling too far behind.
                // Allow for some buffer (e.g., 100ms)
                if (nowTicks > currentTimestamp + (100 * TicksPerMs))
                {
                    _logger.LogWarning($"[BotMediaStream] Redis audio timestamp drifted significantly ({((nowTicks - currentTimestamp) / TicksPerMs)}ms). Resetting timestamp.");
                    currentTimestamp = nowTicks;
                }
                // Also correct if the calculated timestamp is slightly ahead of now (can happen with timing variations)
                else if (currentTimestamp > nowTicks)
                {
                    // _logger.LogTrace($"[BotMediaStream] Calculated timestamp slightly ahead. Adjusting to now.");
                    currentTimestamp = nowTicks;
                }
            }
            _lastRedisAudioTimestamp = currentTimestamp; // Store for the next calculation
            return currentTimestamp;
        }

        // Removed OnAudioMediaReceived method entirely
        // Removed OnSendMediaBuffer method entirely (assuming SpeechService is not used)
    }
}
