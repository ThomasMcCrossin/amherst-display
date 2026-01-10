"""
OCR Engine - Extracts game time from video scoreboards using Tesseract OCR
"""

import logging
import re
from typing import Optional, Tuple, List, Dict
from pathlib import Path
import numpy as np
import cv2
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import pytesseract
    TESSERACT_AVAILABLE = True
except ImportError:
    TESSERACT_AVAILABLE = False
    logging.warning("pytesseract not installed - OCR functionality disabled")

logger = logging.getLogger(__name__)


class OCREngine:
    """Extracts time information from video scoreboards"""

    def __init__(self, config=None):
        """
        Initialize OCR Engine

        Args:
            config: Optional configuration object
        """
        self.config = config
        self.scoreboard_roi: Optional[Tuple[int, int, int, int]] = None  # (x, y, w, h)

        # Validate pytesseract is installed
        if not TESSERACT_AVAILABLE:
            raise RuntimeError(
                "pytesseract not installed. Install with: pip install pytesseract"
            )

        # Validate tesseract-ocr system package is installed
        try:
            pytesseract.get_tesseract_version()
            logger.debug(f"Tesseract version: {pytesseract.get_tesseract_version()}")
        except Exception as e:
            raise RuntimeError(
                "tesseract-ocr system package not found. "
                "Install it:\n"
                "  macOS: brew install tesseract\n"
                "  Ubuntu/Debian: sudo apt-get install tesseract-ocr\n"
                "  Windows: Download from https://github.com/UB-Mannheim/tesseract/wiki\n"
                f"Error: {e}"
            )

    def detect_scoreboard_roi(
        self,
        frame: np.ndarray,
        method: str = 'auto'
    ) -> Optional[Tuple[int, int, int, int]]:
        """
        Detect scoreboard region in frame

        Args:
            frame: Video frame (RGB or BGR)
            method: Detection method ('auto', 'top', 'bottom')

        Returns:
            ROI as (x, y, width, height) or None
        """
        try:
            height, width = frame.shape[:2]

            if method == 'top':
                # Assume scoreboard is in top portion of frame
                return (0, 0, width, int(height * 0.15))

            elif method == 'bottom':
                # Assume scoreboard is in bottom portion
                y_start = int(height * 0.85)
                return (0, y_start, width, height - y_start)

            else:  # 'auto'
                # Default to top 15% of frame (most common for hockey)
                roi = (0, 0, width, int(height * 0.15))
                logger.info(f"Auto-detected scoreboard ROI: {roi}")
                return roi

        except Exception as e:
            logger.error(f"Failed to detect scoreboard ROI: {e}")
            return None

    def set_scoreboard_roi(self, x: int, y: int, width: int, height: int):
        """
        Manually set scoreboard ROI

        Args:
            x: X coordinate
            y: Y coordinate
            width: Width
            height: Height
        """
        self.scoreboard_roi = (x, y, width, height)
        logger.info(f"Scoreboard ROI set to: {self.scoreboard_roi}")

    def extract_time_from_frame(
        self,
        frame: np.ndarray,
        roi: Optional[Tuple[int, int, int, int]] = None
    ) -> Optional[Tuple[int, str]]:
        """
        Extract game time from video frame

        Args:
            frame: Video frame (RGB or BGR)
            roi: Optional region of interest (x, y, w, h). Uses stored ROI if None.

        Returns:
            Tuple of (period, time_string) or None if extraction failed
            Example: (1, "15:23") for Period 1, 15:23 remaining
        """
        if not TESSERACT_AVAILABLE:
            return None

        try:
            # Use provided ROI or stored ROI
            if roi is None:
                roi = self.scoreboard_roi

            # Auto-detect ROI if not set
            if roi is None:
                roi = self.detect_scoreboard_roi(frame)

            if roi is None:
                logger.warning("No ROI available for time extraction")
                return None

            # Extract ROI from frame
            x, y, w, h = roi
            scoreboard = frame[y:y+h, x:x+w]

            # Preprocess for better OCR
            processed = self._preprocess_for_ocr(scoreboard)

            # Run OCR
            text = pytesseract.image_to_string(
                processed,
                config='--psm 6 --oem 3 -c tessedit_char_whitelist=0123456789:. PeriodOT'
            )

            logger.debug(f"OCR raw text: {text}")

            # Parse time and period from text
            result = self._parse_time_text(text)

            if result:
                period, time_str = result
                logger.debug(f"Extracted: Period {period}, Time {time_str}")
                return result

            return None

        except Exception as e:
            logger.error(f"Failed to extract time from frame: {e}")
            return None

    def _preprocess_for_ocr(self, image: np.ndarray) -> np.ndarray:
        """
        Preprocess image for better OCR accuracy

        Args:
            image: Input image (RGB or BGR)

        Returns:
            Preprocessed grayscale image
        """
        try:
            # Convert to grayscale
            if len(image.shape) == 3:
                gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
            else:
                gray = image

            # Resize for better OCR (if too small)
            height = gray.shape[0]
            if height < 50:
                scale = 50 / height
                gray = cv2.resize(gray, None, fx=scale, fy=scale, interpolation=cv2.INTER_CUBIC)

            # Apply bilateral filter to reduce noise while keeping edges sharp
            denoised = cv2.bilateralFilter(gray, 5, 50, 50)

            # Increase contrast using CLAHE (Contrast Limited Adaptive Histogram Equalization)
            clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
            enhanced = clahe.apply(denoised)

            # Apply thresholding
            # Try adaptive thresholding first
            binary = cv2.adaptiveThreshold(
                enhanced,
                255,
                cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                cv2.THRESH_BINARY,
                11,
                2
            )

            return binary

        except Exception as e:
            logger.warning(f"Preprocessing failed, using original: {e}")
            return image

    def _parse_time_text(self, text: str) -> Optional[Tuple[int, str]]:
        """
        Parse period and time from OCR text

        Args:
            text: Raw OCR text

        Returns:
            Tuple of (period, time_string) or None
        """
        # Clean up text
        text = text.strip().upper()

        # Patterns to match
        # Examples: "1st 15:23", "P2 12:00", "Period 3 5:45", "OT 4:23"
        patterns = [
            r'(?:PERIOD\s*)?(\d)[SNRT][TD]?\s*(\d{1,2}:\d{2})',  # "1st 15:23" or "Period 1st 15:23"
            r'P(?:ERIOD)?\s*(\d)\s*(\d{1,2}:\d{2})',             # "P1 15:23" or "Period 1 15:23"
            r'(\d)\s*(\d{1,2}:\d{2})',                           # "1 15:23"
            r'(OT)\s*(\d{1,2}:\d{2})',                           # "OT 4:23"
        ]

        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                period_str, time_str = match.groups()

                # Convert period to int (OT = 4)
                if period_str.upper() == 'OT':
                    period = 4
                else:
                    try:
                        period = int(period_str)
                    except ValueError:
                        continue

                # Validate time format
                if self._validate_time_format(time_str):
                    return (period, time_str)

        # Try to find just a time string if period not found
        time_match = re.search(r'(\d{1,2}:\d{2})', text)
        if time_match:
            time_str = time_match.group(1)
            if self._validate_time_format(time_str):
                # Default to period 1 if we can't determine period
                logger.warning(f"Found time {time_str} but no period - defaulting to P1")
                return (1, time_str)

        logger.debug(f"Could not parse time from: {text}")
        return None

    def _validate_time_format(self, time_str: str) -> bool:
        """
        Validate time string format (MM:SS) and sanity check values

        Args:
            time_str: Time string to validate

        Returns:
            True if valid format and sane values
        """
        try:
            parts = time_str.split(':')
            if len(parts) != 2:
                logger.debug(f"Invalid time format (not MM:SS): {time_str}")
                return False

            minutes = int(parts[0])
            seconds = int(parts[1])

            # Validate seconds range
            if not (0 <= seconds <= 59):
                logger.warning(
                    f"Invalid time '{time_str}' - seconds must be 0-59 (got {seconds})"
                )
                return False

            # Hockey periods are 20 minutes max
            if not (0 <= minutes <= 20):
                logger.warning(
                    f"Invalid time '{time_str}' - hockey periods are 20 minutes max (got {minutes})"
                )
                return False

            return True

        except (ValueError, AttributeError) as e:
            logger.debug(f"Failed to parse time '{time_str}': {e}")
            return False

    def sample_video_times(
        self,
        video_processor,
        sample_interval: int = 30,
        max_samples: Optional[int] = None,
        debug_dir: Optional[Path] = None,
        parallel: bool = True,
        workers: int = 4
    ) -> List[Dict]:
        """
        Sample time from video at regular intervals

        Args:
            video_processor: VideoProcessor instance with loaded video
            sample_interval: Seconds between samples
            max_samples: Maximum number of samples (None for all)
            debug_dir: Optional directory to save debug frames (auto-saves first/middle/last)
            parallel: Whether to use parallel processing (default True)
            workers: Number of worker threads for parallel processing (default 4)

        Returns:
            List of dictionaries with {video_time, period, game_time}
        """
        # Use parallel or sequential implementation based on flag
        if parallel and workers > 1:
            return self._sample_video_times_parallel(
                video_processor,
                sample_interval,
                max_samples,
                debug_dir,
                workers
            )
        else:
            return self._sample_video_times_sequential(
                video_processor,
                sample_interval,
                max_samples,
                debug_dir
            )

    def _sample_video_times_sequential(
        self,
        video_processor,
        sample_interval: int = 30,
        max_samples: Optional[int] = None,
        debug_dir: Optional[Path] = None
    ) -> List[Dict]:
        """
        Sample time from video sequentially (original implementation)

        Args:
            video_processor: VideoProcessor instance with loaded video
            sample_interval: Seconds between samples
            max_samples: Maximum number of samples (None for all)
            debug_dir: Optional directory to save debug frames (auto-saves first/middle/last)

        Returns:
            List of dictionaries with {video_time, period, game_time}
        """
        timestamps = []

        try:
            duration = video_processor.duration

            # Calculate total number of samples for progress bar
            total_samples = int(duration / sample_interval) + 1
            if max_samples:
                total_samples = min(total_samples, max_samples)

            # Determine which samples to save as debug frames
            debug_sample_indices = set()
            if total_samples > 0:
                debug_sample_indices = {
                    0,                          # First sample
                    total_samples // 2,         # Middle sample
                    total_samples - 1           # Last sample
                }

            # Create progress bar
            progress_bar = tqdm(
                total=total_samples,
                desc="OCR Sampling",
                unit="frame",
                ncols=100
            )

            current_time = 0.0
            sample_count = 0

            while current_time < duration:
                # Check max samples limit
                if max_samples and sample_count >= max_samples:
                    break

                # Get frame at current time
                frame = video_processor.get_frame_at_time(current_time)

                if frame is not None:
                    # Save debug frame for first, middle, and last samples
                    if debug_dir and sample_count in debug_sample_indices:
                        roi = self.scoreboard_roi or self.detect_scoreboard_roi(frame)
                        debug_path = debug_dir / f"debug_ocr_frame_{sample_count:04d}_{current_time:.1f}s.jpg"
                        self.save_debug_frame(frame, debug_path, roi)
                        logger.debug(f"Saved debug frame: {debug_path}")

                    # Extract time from frame
                    result = self.extract_time_from_frame(frame)

                    if result:
                        period, game_time = result
                        timestamps.append({
                            'video_time': current_time,
                            'period': period,
                            'game_time': game_time,
                            'game_time_seconds': self._time_to_seconds(game_time)
                        })
                        logger.debug(f"Sample at {current_time:.1f}s: P{period} {game_time}")
                        # Update progress bar description with latest result
                        progress_bar.set_postfix({'latest': f"P{period} {game_time}"})

                # Update progress bar
                progress_bar.update(1)

                # Move to next sample
                current_time += sample_interval
                sample_count += 1

            # Close progress bar
            progress_bar.close()

            logger.info(f"Sampled {len(timestamps)} timestamps from video")
            if debug_dir and debug_sample_indices:
                logger.info(f"Debug frames saved to: {debug_dir}")

            return timestamps

        except Exception as e:
            logger.error(f"Failed to sample video times: {e}")
            return []

    def _sample_video_times_parallel(
        self,
        video_processor,
        sample_interval: int = 30,
        max_samples: Optional[int] = None,
        debug_dir: Optional[Path] = None,
        workers: int = 4
    ) -> List[Dict]:
        """
        Sample time from video in parallel using ThreadPoolExecutor

        Args:
            video_processor: VideoProcessor instance with loaded video
            sample_interval: Seconds between samples
            max_samples: Maximum number of samples (None for all)
            debug_dir: Optional directory to save debug frames (auto-saves first/middle/last)
            workers: Number of worker threads (default 4)

        Returns:
            List of dictionaries with {video_time, period, game_time}
        """
        timestamps = []

        try:
            duration = video_processor.duration

            # Calculate all sample times
            sample_times = []
            current_time = 0.0
            while current_time < duration:
                sample_times.append(current_time)
                current_time += sample_interval
                if max_samples and len(sample_times) >= max_samples:
                    break

            total_samples = len(sample_times)

            # Determine which samples to save as debug frames
            debug_sample_indices = set()
            if total_samples > 0:
                debug_sample_indices = {
                    0,                          # First sample
                    total_samples // 2,         # Middle sample
                    total_samples - 1           # Last sample
                }

            logger.info(f"Starting parallel OCR sampling with {workers} workers")
            logger.debug(f"Total samples to process: {total_samples}")

            # Create progress bar
            progress_bar = tqdm(
                total=total_samples,
                desc=f"OCR Sampling ({workers} workers)",
                unit="frame",
                ncols=100
            )

            # Process frames in parallel
            with ThreadPoolExecutor(max_workers=workers) as executor:
                # Submit all tasks
                future_to_sample = {}
                for idx, sample_time in enumerate(sample_times):
                    save_debug = debug_dir is not None and idx in debug_sample_indices
                    future = executor.submit(
                        self._extract_time_at_sample,
                        video_processor,
                        sample_time,
                        idx,
                        save_debug,
                        debug_dir
                    )
                    future_to_sample[future] = (idx, sample_time)

                # Collect results as they complete
                for future in as_completed(future_to_sample):
                    idx, sample_time = future_to_sample[future]

                    try:
                        result = future.result()
                        if result:
                            timestamps.append(result)
                            period, game_time = result['period'], result['game_time']
                            progress_bar.set_postfix({'latest': f"P{period} {game_time}"})
                        else:
                            progress_bar.set_postfix({'status': 'no_data'})

                    except Exception as exc:
                        logger.warning(f"Sample at {sample_time:.1f}s failed: {exc}")
                        progress_bar.set_postfix({'status': 'error'})

                    # Update progress bar
                    progress_bar.update(1)

            # Close progress bar
            progress_bar.close()

            # Sort timestamps by video_time
            timestamps.sort(key=lambda t: t['video_time'])

            logger.info(f"Sampled {len(timestamps)} timestamps from video (parallel)")
            if debug_dir and debug_sample_indices:
                logger.info(f"Debug frames saved to: {debug_dir}")

            return timestamps

        except Exception as e:
            logger.error(f"Failed to sample video times (parallel): {e}")
            return []

    def _extract_time_at_sample(
        self,
        video_processor,
        sample_time: float,
        sample_idx: int,
        save_debug: bool,
        debug_dir: Optional[Path]
    ) -> Optional[Dict]:
        """
        Helper method to extract time at a specific sample position (thread-safe)

        Args:
            video_processor: VideoProcessor instance
            sample_time: Time in video to sample
            sample_idx: Index of this sample
            save_debug: Whether to save debug frame
            debug_dir: Directory for debug frames

        Returns:
            Dictionary with timestamp data or None if extraction failed
        """
        try:
            # Get frame at current time
            frame = video_processor.get_frame_at_time(sample_time)

            if frame is None:
                return None

            # Save debug frame if requested
            if save_debug and debug_dir:
                roi = self.scoreboard_roi or self.detect_scoreboard_roi(frame)
                debug_path = debug_dir / f"debug_ocr_frame_{sample_idx:04d}_{sample_time:.1f}s.jpg"
                self.save_debug_frame(frame, debug_path, roi)
                logger.debug(f"Saved debug frame: {debug_path}")

            # Extract time from frame
            result = self.extract_time_from_frame(frame)

            if result:
                period, game_time = result
                return {
                    'video_time': sample_time,
                    'period': period,
                    'game_time': game_time,
                    'game_time_seconds': self._time_to_seconds(game_time)
                }

            return None

        except Exception as e:
            logger.debug(f"Failed to extract time at {sample_time:.1f}s: {e}")
            return None

    def _time_to_seconds(self, time_str: str) -> int:
        """
        Convert MM:SS time string to seconds

        Args:
            time_str: Time string in MM:SS format

        Returns:
            Time in seconds
        """
        try:
            parts = time_str.split(':')
            if len(parts) == 2:
                minutes = int(parts[0])
                seconds = int(parts[1])
                return minutes * 60 + seconds
        except (ValueError, AttributeError):
            pass

        return 0

    def save_debug_frame(self, frame: np.ndarray, output_path: Path, roi: Optional[Tuple] = None):
        """
        Save frame with ROI highlighted for debugging

        Args:
            frame: Video frame
            output_path: Where to save image
            roi: Optional ROI to highlight
        """
        try:
            # Make a copy
            debug_frame = frame.copy()

            # Draw ROI if provided
            if roi:
                x, y, w, h = roi
                cv2.rectangle(debug_frame, (x, y), (x+w, y+h), (0, 255, 0), 2)

            # Save
            cv2.imwrite(str(output_path), debug_frame)
            logger.info(f"Saved debug frame to {output_path}")

        except Exception as e:
            logger.error(f"Failed to save debug frame: {e}")
