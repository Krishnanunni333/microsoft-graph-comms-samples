// ***********************************************************************
// Assembly          : EchoBot.Services
// Author            : Krishnanunni (Modified for Redis)
// Created           : 09-07-2020
//
// Last Modified By  : Krishnanunni
// Last Modified On  : 2025-04-17 // Adjusted date
// ***********************************************************************
// <copyright file="BotMediaStream.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.
// </copyright>
// <summary>The bot media stream modified to play audio and video from Redis.</summary>
// ***********************************************************************-
using EchoBot.Util;
using EchoBot.Media;
using Microsoft.Graph.Communications.Calls;
using Microsoft.Graph.Communications.Calls.Media;
using Microsoft.Graph.Communications.Common;
using Microsoft.Graph.Communications.Common.Telemetry;
using Microsoft.Skype.Bots.Media;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

// Ensure you have a using directive for your logger implementation (e.g., Microsoft.Extensions.Logging)
using Microsoft.Extensions.Logging;
using System.Runtime.InteropServices;
using Microsoft.Skype.Internal.Media.Services.Common; // Needed for GCHandle/Marshal if used in Utilities
// using Microsoft.Skype.Internal.Media.Services.Common; // Likely not needed

namespace EchoBot.Bot
{
    /// <summary>
    /// Class responsible for streaming audio and video received from Redis channels.
    /// </summary>
    public class BotMediaStream : ObjectRootDisposable
    {
        // Re-added participants list as per user's last code block
        internal List<IParticipant> participants;
        private readonly IAudioSocket _audioSocket;
        private readonly IVideoSocket _mainVideoSocket; // Make nullable explicit

        /// <summary>
        /// Contains a map from simple color/width/height combinations to VideoFormat objects.
        /// </summary>
        public static readonly Dictionary<(VideoColorFormat format, int width, int height), VideoFormat> VideoFormatMap = new Dictionary<(VideoColorFormat format, int width, int height), VideoFormat>()
        {
             { (VideoColorFormat.NV12, 1920, 1080), VideoFormat.NV12_1920x1080_15Fps },
             { (VideoColorFormat.NV12, 1280, 720), VideoFormat.NV12_1280x720_15Fps },
             { (VideoColorFormat.NV12, 640, 360), VideoFormat.NV12_640x360_15Fps },
             { (VideoColorFormat.NV12, 480, 270), VideoFormat.NV12_480x270_15Fps },
             { (VideoColorFormat.NV12, 424, 240), VideoFormat.NV12_424x240_15Fps },
             { (VideoColorFormat.NV12, 360, 640), VideoFormat.NV12_360x640_15Fps },
             { (VideoColorFormat.NV12, 320, 180), VideoFormat.NV12_320x180_15Fps },
             { (VideoColorFormat.NV12, 270, 480), VideoFormat.NV12_270x480_15Fps },
             { (VideoColorFormat.NV12, 240, 424), VideoFormat.NV12_240x424_15Fps },
             { (VideoColorFormat.NV12, 180, 320), VideoFormat.NV12_180x320_30Fps },
        };
        private readonly AppSettings _settings;
        private readonly ILogger _logger;
        private AudioVideoFramePlayer _audioVideoFramePlayer;
        private readonly TaskCompletionSource<bool> _audioSendStatusActive;
        private readonly TaskCompletionSource<bool> _videoSendStatusActive;
        private readonly TaskCompletionSource<bool> _startAVPlayerCompleted;
        private AudioVideoFramePlayerSettings _audioVideoFramePlayerSettings;
        private int _shutdown;
        private List<AudioMediaBuffer> _audioMediaBuffers = new List<AudioMediaBuffer>();
        private List<VideoMediaBuffer> _videoMediaBuffers = new List<VideoMediaBuffer>();

        // --- Redis Members ---
        private ConnectionMultiplexer _redisConnection;
        private ISubscriber _redisSubscriber;
        private const string RedisAudioChannelName = "audio_stream";
        private const string RedisVideoChannelName = "video_stream";
        private const string RedisAudioPushChannelName = "audio_push_stream";
        private long _lastRedisAudioTimestamp = -1;
        private const long TicksPerMs = TimeSpan.TicksPerMillisecond;
        private const int AudioChunkDurationMs = 20;
        private long audioTick;
        private long videoTick;
        private long mediaTick;
        private readonly object mLock = new object();

        /// <summary>
        /// Initializes a new instance of the <see cref="BotMediaStream" /> class.
        /// Connects to Redis and prepares to play audio/video from the specified channels.
        /// </summary>
        public BotMediaStream(
            ILocalMediaSession mediaSession,
            string callId,
            IGraphLogger graphLogger,
            ILogger logger,
            AppSettings settings
        )
            : base(graphLogger)
        {
            ArgumentVerifier.ThrowOnNullArgument(mediaSession, nameof(mediaSession));
            ArgumentVerifier.ThrowOnNullArgument(logger, nameof(logger));
            ArgumentVerifier.ThrowOnNullArgument(settings, nameof(settings));

            _settings = settings;
            _logger = logger;
            // Use RunContinuationsAsynchronously for TaskCompletionSource
            _audioSendStatusActive = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            _videoSendStatusActive = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            _startAVPlayerCompleted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            // Initialize participants list (if needed)
            this.participants = new List<IParticipant>();

            this._audioSocket = mediaSession.AudioSocket;
            if (this._audioSocket == null)
            {
                _logger.LogError("Media session does not have an audio socket.");
                throw new InvalidOperationException("A mediaSession needs to have at least an audioSocket to send audio.");
            }
            this._audioSocket.AudioSendStatusChanged += OnAudioSendStatusChanged;
            this._audioSocket.AudioMediaReceived += OnAudioMediaReceived;


            this._mainVideoSocket = mediaSession.VideoSockets?.FirstOrDefault();
            if (this._mainVideoSocket != null)
            {
                _logger.LogInformation("Video socket found. Subscribing to video events.");
                this._mainVideoSocket.VideoSendStatusChanged += this.OnVideoSendStatusChanged;
                this._mainVideoSocket.VideoKeyFrameNeeded += this.OnVideoKeyFrameNeeded;
            }

            InitializeRedis();
        }

        /// <summary>
        /// Cleans up resources, unsubscribing from Redis and shutting down the player.
        /// </summary>
        public async Task ShutdownAsync()
        {
            if (Interlocked.CompareExchange(ref this._shutdown, 1, 0) == 1) return;

            _logger.LogInformation("[BotMediaStream] Initiating shutdown...");

            // --- Redis Cleanup ---
            if (_redisSubscriber != null)
            {
                try
                {
                    _logger.LogInformation($"[BotMediaStream] Unsubscribing from Redis channels: {RedisAudioChannelName} and {RedisVideoChannelName}");
                    // Use ConfigureAwait(false) to avoid deadlocks in certain contexts
                    await _redisSubscriber.UnsubscribeAsync(RedisAudioChannelName).ConfigureAwait(false);
                    await _redisSubscriber.UnsubscribeAsync(RedisVideoChannelName).ConfigureAwait(false);
                    await _redisSubscriber.UnsubscribeAsync(RedisAudioPushChannelName).ConfigureAwait(false);
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

            // Wait for the player to have been created before trying to shut it down
            //await this._startAVPlayerCompleted.Task.ConfigureAwait(false);

            // Unsubscribe from the socket event
            if (this._audioSocket != null)
            {
                this._audioSocket.AudioSendStatusChanged -= this.OnAudioSendStatusChanged;
                this._audioSocket.AudioMediaReceived -= this.OnAudioMediaReceived;
            }

            // Unsubscribe from the video socket event
            if (this._mainVideoSocket != null)
            {
                this._mainVideoSocket.VideoKeyFrameNeeded -= this.OnVideoKeyFrameNeeded;
                this._mainVideoSocket.VideoSendStatusChanged -= this.OnVideoSendStatusChanged;
            }
            // Shut down the media player
            if (this._audioVideoFramePlayer != null)
            {
                _logger.LogInformation("[BotMediaStream] Shutting down AudioVideoFramePlayer.");
                await this._audioVideoFramePlayer.ShutdownAsync().ConfigureAwait(false);
                this._audioVideoFramePlayer = null; // Release reference
            }

            _logger.LogInformation("[BotMediaStream] Shutdown completed.");

            foreach (var audioMediaBuffer in this._audioMediaBuffers)
            {
                audioMediaBuffer.Dispose();
            }

            _logger.LogInformation($"disposed {this._audioMediaBuffers.Count} audioMediaBUffers.");
            foreach (var videoMediaBuffer in this._videoMediaBuffers)
            {
                videoMediaBuffer.Dispose();
            }
            this._audioMediaBuffers.Clear();
            this._videoMediaBuffers.Clear();
            // Dispose base class resources
            base.Dispose(true); // Assuming true indicates disposing managed resources
            GC.SuppressFinalize(this); // Prevent finalizer from running

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

        private void OnVideoSendStatusChanged(object? sender, VideoSendStatusChangedEventArgs e)
        {
            _logger.LogInformation($"[BotMediaStream] VideoSendStatus changed to {e.MediaSendStatus}. Preferred Format: {e.PreferredVideoSourceFormat}");
            if (e.MediaSendStatus == MediaSendStatus.Active)
            {
                this._videoSendStatusActive.TrySetResult(true);
            }
            else if (e.MediaSendStatus == MediaSendStatus.Inactive)
            {
                _logger.LogWarning("[BotMediaStream] VideoSendStatus is Inactive.");
                this._videoSendStatusActive.TrySetException(new InvalidOperationException("VideoSendStatus became Inactive."));
            }
        }

        /// <summary>
        /// Establishes the connection to the Redis server.
        /// </summary>
        private void InitializeRedis()
        {
            try
            {
                // Ensure existing connection is cleaned up if called again (unlikely but safe)
                if (_redisConnection != null)
                {
                    _logger.LogWarning("[BotMediaStream] InitializeRedisConnection called with existing connection. Disposing old one.");
                    _redisConnection.Close(false);
                    _redisConnection.Dispose();
                    _redisConnection = null;
                }

                string redisConnectionString = _settings.RedisConnectionString ?? "localhost:6379,abortConnect=False,connectTimeout=30000,responseTimeout=30000";
                _logger.LogInformation($"[BotMediaStream] Attempting to connect to Redis: {redisConnectionString.Split(',')[0]}...");

                var options = ConfigurationOptions.Parse(redisConnectionString);
                options.AbortOnConnectFail = false; // Don't throw immediately if connection fails
                //options.AsyncTimeout = 5000;       // Example: 5 second timeout for async operations
                //options.ConnectTimeout = 5000;     // Example: 5 second timeout for initial connection

                _redisConnection = ConnectionMultiplexer.Connect(options);

                // Log connection events for debugging
                _redisConnection.ConnectionFailed += (sender, args) =>
                {
                    _logger.LogError($"[BotMediaStream] Redis connection failed: {args.FailureType}, Endpoint: {args.EndPoint}, Exception: {args.Exception?.Message}");
                };
                _redisConnection.ConnectionRestored += (sender, args) =>
                {
                    _logger.LogInformation($"[BotMediaStream] Redis connection restored: {args.FailureType}, Endpoint: {args.EndPoint}");
                    // Re-subscribe when connection is restored
                    SubscribeToChannels();
                };
                _redisConnection.ErrorMessage += (sender, args) =>
                {
                    _logger.LogError($"[BotMediaStream] Redis error message: {args.Message}");
                };

                if (_redisConnection.IsConnected)
                {
                    _logger.LogInformation("[BotMediaStream] Successfully established initial Redis connection.");
                    SubscribeToChannels();
                }
                else
                {
                    _logger.LogWarning("[BotMediaStream] Initial Redis connection failed. Background reconnection attempts will start.");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[BotMediaStream] An unexpected error occurred during Redis connection initialization.");
            }
        }

        /// <summary>
        /// Subscribes to the necessary Redis channels using the existing subscriber.
        /// </summary>
        private void SubscribeToChannels()
        {
            if (_redisConnection == null || !_redisConnection.IsConnected)
            {
                _logger.LogWarning("[BotMediaStream] Cannot subscribe, Redis connection is not available.");
                return;
            }
            // Avoid re-subscribing if already subscribed
            if (_redisSubscriber != null && _redisSubscriber.IsConnected(RedisVideoChannelName) && _redisSubscriber.IsConnected(RedisAudioChannelName))
            {
                _logger.LogInformation($"[BotMediaStream] Already subscribed to Redis channel: {RedisAudioChannelName}, {RedisVideoChannelName}");
                return;
            }

            try
            {
                _redisSubscriber = _redisConnection.GetSubscriber();

                // Subscribe to the channel, directing messages to OnRedisAudioReceived
                // Use SubscribeAsync for consistency, although fire-and-forget is often acceptable here.
                _redisSubscriber.Subscribe(RedisAudioChannelName, OnRedisAudioReceived); // Simple sync-over-async subscribe

                _logger.LogInformation($"[BotMediaStream] Subscribed to Redis channel: {RedisAudioChannelName}");

                _redisSubscriber.Subscribe(RedisVideoChannelName, OnRedisVideoReceived); // Simple sync-over-async subscribe

                _logger.LogInformation($"[BotMediaStream] Subscribed to Redis channel: {RedisVideoChannelName}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"[BotMediaStream] Failed to subscribe to Redis channels.");
            }
        }


        private async void OnRedisVideoReceived(RedisChannel channel, RedisValue message)
        {
            if (this._shutdown == 1) return;

            try
            {
                if (!message.HasValue || message.IsNullOrEmpty) return;
                byte[] videoChunk = (byte[])message;
                if (videoChunk.Length <= 3 && System.Text.Encoding.UTF8.GetString(videoChunk) == "EOS")
                {
                    _logger.LogInformation("[BotMediaStream] Received video EOS marker.");
                    return;
                }

                // IntPtr unmanagedPointer = Marshal.AllocHGlobal(videoChunk.Length);
                // var nv12 = this.BGRAtoNV12(unmanagedPointer, 640, 360);
                // var format = VideoFormatMap[(VideoColorFormat.NV12, 640, 360)];
                // this.SendVideo(new VideoSendBuffer(nv12, (uint)nv12.Length, format));
                IntPtr unmanagedPointer = Marshal.AllocHGlobal(videoChunk.Length);
                Marshal.Copy(videoChunk, 0, unmanagedPointer, videoChunk.Length);
                var format = VideoFormatMap[(VideoColorFormat.NV12, 640, 360)];
                this.SendVideo(new VideoSendBuffer(unmanagedPointer, videoChunk.Length, format));
            }
            catch (TimeoutException) { _logger.LogWarning("[BotMediaStream] Timed out waiting for player ready in OnRedisVideoReceived."); }
            catch (ObjectDisposedException) { _logger.LogWarning("[BotMediaStream] Object disposed in OnRedisVideoReceived (likely player or task)."); }
            catch (OperationCanceledException) { _logger.LogInformation("[BotMediaStream] Operation canceled in OnRedisVideoReceived (likely shutdown)."); }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[BotMediaStream] Error processing video chunk from Redis.");
            }
        }

        /// <summary>
        /// Sends a <see cref="VideoMediaBuffer"/> to the call from the Bot's video feed.
        /// </summary>
        /// <param name="buffer">The video buffer to send.</param>
        private void SendVideo(VideoMediaBuffer buffer)
        {
            // Send the video to our outgoing video stream
            try
            {
                this._mainVideoSocket.Send(buffer);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"[OnVideoMediaReceived] Exception while calling mainVideoSocket.Send()");
            }
        }

        private async void OnRedisAudioReceived(RedisChannel channel, RedisValue message)
        {
            if (this._shutdown == 1) return;
            try
            {
                if (!message.HasValue || message.IsNullOrEmpty) return;
                byte[] audioChunk = (byte[])message;
                if (audioChunk.Length <= 3 && System.Text.Encoding.UTF8.GetString(audioChunk) == "EOS")
                {
                    _logger.LogInformation("[BotMediaStream] Received audio EOS marker.");
                    return;
                }


                IntPtr unmanagedPointer = Marshal.AllocHGlobal(audioChunk.Length);
                Marshal.Copy(audioChunk, 0, unmanagedPointer, audioChunk.Length);
                this.SendAudio(new AudioSendBuffer(unmanagedPointer, audioChunk.Length, AudioFormat.Pcm16K));


            }
            catch (TimeoutException) { _logger.LogWarning("[BotMediaStream] Timed out waiting for player ready in OnRedisAudioReceived."); }
            catch (ObjectDisposedException) { _logger.LogWarning("[BotMediaStream] Object disposed in OnRedisAudioReceived."); }
            catch (OperationCanceledException) { _logger.LogInformation("[BotMediaStream] Operation canceled in OnRedisAudioReceived."); }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[BotMediaStream] Error processing audio chunk from Redis.");
            }
        }

        /// <summary>
        /// Handles audio received FROM Teams participants. ***
        /// Copies the audio data and publishes it to the Redis push channel.
        /// </summary>
        private void OnAudioMediaReceived(object? sender, AudioMediaReceivedEventArgs e)
        {
            // Immediately dispose buffer if shutting down or no Redis connection
            if (_shutdown == 1) { e.Buffer.Dispose(); return; }

            ISubscriber? subscriber = _redisSubscriber;
            if (subscriber == null || _redisConnection == null || !_redisConnection.IsConnected)
            {
                _logger.LogWarning("[BotMediaStream::OnAudioMediaReceived] Skipping received audio frame: Redis connection or subscriber is not available/ready.");
                e.Buffer.Dispose();
                return;
            }

            try
            {
                var bufferLength = e.Buffer.Length;
                if (bufferLength <= 0)
                {
                    e.Buffer.Dispose();
                    return;
                }


                var buffer = new byte[bufferLength];

                Marshal.Copy(e.Buffer.Data, buffer, 0, (int)bufferLength);


                long clientsReceived = subscriber.Publish(RedisAudioPushChannelName, buffer, CommandFlags.FireAndForget);
                _logger.LogTrace($"[BotMediaStream::OnAudioMediaReceived] Received {bufferLength} audio bytes from Teams. Published to {clientsReceived} subscribers on '{RedisAudioPushChannelName}'.");
            }
            catch (ObjectDisposedException odEx)
            {
                _logger.LogWarning(odEx, "[BotMediaStream::OnAudioMediaReceived] Object disposed exception (likely Redis subscriber during publish).");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[BotMediaStream::OnAudioMediaReceived] Error processing or publishing received audio chunk from Teams.");
            }
            finally
            {
                // **CRITICAL:** Always dispose the buffer provided by the event args
                e.Buffer.Dispose();
            }
        }

        /// <summary>
        /// Sends an <see cref="AudioMediaBuffer"/> to the call from the Bot's audio feed.
        /// </summary>
        /// <param name="buffer">The audio buffer to send.</param>
        private void SendAudio(AudioMediaBuffer buffer)
        {
            // Send the audio to our outgoing video stream
            try
            {
                this._audioSocket.Send(buffer);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"[OnAudioReceived] Exception while calling audioSocket.Send()");
            }
        }


        // --- Other Methods (OnVideoKeyFrameNeeded, GenerateTimestamp) ---

        private void OnVideoKeyFrameNeeded(object? sender, VideoKeyFrameNeededEventArgs e)
        {
            _logger.LogInformation($"[VideoKeyFrameNeeded(MediaType={e.MediaType}; SocketId={e.SocketId}; Formats={string.Join(";", e.VideoFormats.Select(vf => vf.VideoColorFormat))})]");
            // No action needed typically for playback, but log is useful.
        }

    }
}