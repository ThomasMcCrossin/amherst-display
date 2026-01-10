"""
Video Processor - Handles video loading, clip creation, and rendering
"""

import logging
from pathlib import Path
from typing import List, Optional, Tuple
import numpy as np
from tqdm import tqdm

try:
    from moviepy import VideoFileClip, concatenate_videoclips
except ImportError:
    from moviepy.editor import VideoFileClip, concatenate_videoclips

logger = logging.getLogger(__name__)


class VideoProcessor:
    """Handles all video-related operations"""

    def __init__(self, video_path: Path, config):
        """
        Initialize VideoProcessor

        Args:
            video_path: Path to video file
            config: Configuration module
        """
        self.video_path = video_path
        self.config = config
        self.video_clip: Optional[VideoFileClip] = None
        self.duration: float = 0.0
        self.fps: float = 0.0

    def load_video(self) -> bool:
        """
        Load video file

        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Loading video: {self.video_path}")

            self.video_clip = VideoFileClip(str(self.video_path))
            self.duration = self.video_clip.duration
            self.fps = self.video_clip.fps

            logger.info(f"Video loaded: {self.duration:.1f}s @ {self.fps:.1f} FPS")
            return True

        except Exception as e:
            logger.error(f"Failed to load video: {e}")
            return False

    def get_frame_at_time(self, time_seconds: float) -> Optional[np.ndarray]:
        """
        Extract a single frame at specified time

        Args:
            time_seconds: Time in seconds

        Returns:
            Frame as numpy array (RGB) or None if failed
        """
        if self.video_clip is None:
            logger.error("No video loaded")
            return None

        try:
            # Ensure time is within bounds
            time_seconds = max(0, min(time_seconds, self.duration))
            frame = self.video_clip.get_frame(time_seconds)
            return frame

        except Exception as e:
            logger.error(f"Failed to get frame at {time_seconds}s: {e}")
            return None

    def create_clip(
        self,
        start_time: float,
        end_time: float,
        output_path: Optional[Path] = None
    ) -> Optional[VideoFileClip]:
        """
        Create a video clip between start and end times

        Args:
            start_time: Start time in seconds
            end_time: End time in seconds
            output_path: Optional path to save clip

        Returns:
            VideoFileClip object or None if failed
        """
        if self.video_clip is None:
            logger.error("No video loaded")
            return None

        try:
            # Ensure times are within bounds
            start_time = max(0, min(start_time, self.duration))
            end_time = max(start_time, min(end_time, self.duration))

            logger.debug(f"Creating clip: {start_time:.1f}s - {end_time:.1f}s")

            # Create subclip
            clip = self.video_clip.subclipped(start_time, end_time)

            # Save if output path provided
            if output_path:
                self._write_video(clip, output_path)

            return clip

        except Exception as e:
            logger.error(f"Failed to create clip: {e}")
            return None

    def create_highlight_clips(
        self,
        events: List[dict],
        clips_dir: Path,
        before_seconds: float = 8.0,
        after_seconds: float = 6.0
    ) -> List[Tuple[dict, Path]]:
        """
        Create highlight clips for each event

        Args:
            events: List of event dictionaries with 'video_time' key
            clips_dir: Directory to save clips
            before_seconds: Seconds to include before event
            after_seconds: Seconds to include after event

        Returns:
            List of tuples (event, clip_path)
        """
        if self.video_clip is None:
            logger.error("No video loaded")
            return []

        created_clips = []

        # Create progress bar for clip creation
        progress_bar = tqdm(
            events,
            desc="Creating Clips",
            unit="clip",
            ncols=100
        )

        for i, event in enumerate(progress_bar, 1):
            try:
                video_time = event.get('video_time')
                if video_time is None:
                    logger.warning(f"Event {i} missing video_time")
                    progress_bar.set_postfix({'status': 'skipped'})
                    continue

                # Calculate clip boundaries
                start_time = max(0, video_time - before_seconds)
                end_time = min(self.duration, video_time + after_seconds)

                # Create clip filename
                event_type = event.get('type', 'event').upper()
                period = event.get('period', 0)
                team = event.get('team', 'unknown').replace(' ', '_')

                clip_filename = f"{i:02d}_{event_type}_P{period}_{team}.mp4"
                clip_path = clips_dir / clip_filename

                # Update progress bar with current clip info
                progress_bar.set_postfix({'clip': clip_filename[:30]})

                # Create and save clip
                logger.debug(f"Creating clip {i}/{len(events)}: {clip_filename}")

                clip = self.create_clip(start_time, end_time, clip_path)

                if clip:
                    created_clips.append((event, clip_path))
                    # Close clip to free memory
                    clip.close()
                    progress_bar.set_postfix({'status': 'done'})

            except Exception as e:
                logger.error(f"Failed to create clip {i}: {e}")
                progress_bar.set_postfix({'status': 'error'})
                continue

        # Close progress bar
        progress_bar.close()

        logger.info(f"Created {len(created_clips)}/{len(events)} highlight clips")
        return created_clips

    def create_highlights_reel(
        self,
        clip_paths: List[Path],
        output_path: Path,
        max_clips: Optional[int] = None
    ) -> bool:
        """
        Concatenate multiple clips into a single highlights reel

        Args:
            clip_paths: List of clip file paths
            output_path: Path for final highlights video
            max_clips: Maximum number of clips to include (None for all)

        Returns:
            True if successful, False otherwise
        """
        if not clip_paths:
            logger.warning("No clips provided for highlights reel")
            return False

        try:
            # Limit number of clips if specified
            if max_clips:
                clip_paths = clip_paths[:max_clips]

            logger.info(f"Creating highlights reel from {len(clip_paths)} clips")

            # Load all clips with progress bar
            clips = []
            progress_bar = tqdm(
                clip_paths,
                desc="Loading Clips",
                unit="clip",
                ncols=100
            )

            for clip_path in progress_bar:
                try:
                    progress_bar.set_postfix({'file': clip_path.name[:30]})
                    clip = VideoFileClip(str(clip_path))
                    clips.append(clip)
                except Exception as e:
                    logger.warning(f"Failed to load clip {clip_path}: {e}")
                    progress_bar.set_postfix({'status': 'error'})

            progress_bar.close()

            if not clips:
                logger.error("No clips could be loaded")
                return False

            # Concatenate clips
            logger.info("Concatenating clips...")
            final_clip = concatenate_videoclips(clips, method="compose")

            # Write final video
            logger.info(f"Writing highlights reel to {output_path}")
            self._write_video(final_clip, output_path)

            # Cleanup
            final_clip.close()
            for clip in clips:
                clip.close()

            logger.info(f"✅ Highlights reel created: {output_path}")
            return True

        except Exception as e:
            logger.error(f"Failed to create highlights reel: {e}")
            return False

    def _write_video(self, clip: VideoFileClip, output_path: Path):
        """
        Write video clip to file with proper codec settings

        Args:
            clip: VideoFileClip object
            output_path: Output file path
        """
        try:
            # Ensure parent directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Write video with codec settings from config
            codec = getattr(self.config, 'OUTPUT_CODEC', 'mpeg4')

            # Try different write methods for MoviePy 1.x vs 2.x compatibility
            try:
                clip.write_videofile(
                    str(output_path),
                    codec=codec,
                    audio_codec='aac',
                    temp_audiofile=None,
                    remove_temp=True,
                    logger=None
                )
            except TypeError:
                # MoviePy 2.x may not accept logger parameter
                clip.write_videofile(
                    str(output_path),
                    codec=codec,
                    audio_codec='aac',
                    temp_audiofile=None,
                    remove_temp=True
                )

            logger.debug(f"Video written to {output_path}")

        except Exception as e:
            logger.error(f"Failed to write video to {output_path}: {e}")
            raise

    def cleanup(self):
        """Close video clip and free resources"""
        if self.video_clip:
            try:
                self.video_clip.close()
                logger.debug("Video clip closed")
            except Exception as e:
                logger.warning(f"Error closing video clip: {e}")

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.cleanup()
