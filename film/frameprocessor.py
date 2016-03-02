#!/usr/bin/env python
import os
from argparse import ArgumentParser

from moviepy.editor import VideoFileClip
import numpy as np
from PIL import Image
from tqdm import tqdm


def frange(x, y, inc):
    while x < y:
        yield x
        x += inc


def average_video(filepath, outpath, start=None, end=None, sample_every=1):
    """Calculate average of video frames"""

    # Load video
    vid = VideoFileClip(filepath, audio=False)
    width = vid.w
    height = vid.h

    if start is None and end is None:
        frame_generator = vid.iter_frames(progress_bar=True, dtype=np.uint8)
    else:
        if start is None:
            start = 0
        if end is None:
            end = vid.duration
        # compute time increment for sampling by frames
        sample_inc = sample_every / vid.fps
        frame_generator = tqdm(vid.get_frame(f) for f in frange(start, end, sample_inc))

    # create starting matrix of zeros
    sum_fs = np.zeros(shape=(height, width, 3), dtype=int)
    ma_sum_fs = np.zeros(shape=(height, width, 3), dtype=int)
    prev_f = np.zeros(shape=(height, width, 3), dtype=int)
    sum_delta_fs = np.zeros(shape=(height, width, 3), dtype=int)

    n_frames = 0
    for f in frame_generator:
        delta = f - prev_f
        sum_delta_fs += delta
        sum_fs += f

        ma_sum_fs += f
        if divmod(n_frames, 100)[1] == 0 and n_frames > 0:
            ma_f = ma_sum_fs / 100
            Image.fromarray(ma_f.astype(np.uint8))\
                .save(os.path.join(outpath, 'movavg_{}.png'.format(n_frames)))
            ma_sum_fs = np.zeros(shape=(height, width, 3), dtype=int)

        n_frames += 1
        prev_f = f

    # average out the values for each frame
    average_delta_f = sum_delta_fs / n_frames
    average_f = sum_fs / n_frames

    # Create images
    delta_img = Image.fromarray(average_delta_f.astype(np.uint8))
    delta_img.save(os.path.join(outpath, 'average_delta.png'))
    final_img = Image.fromarray(average_f.astype(np.uint8))
    final_img.save(os.path.join(outpath, 'average.png'))


if __name__ == "__main__":
    parser = ArgumentParser(description="Creates image with averaged pixels"
                                        "for a given movie clip")
    parser.add_argument("-i", required=True, type=str,
                        help="filepath to movie clip")
    parser.add_argument("-o", required=True, type=str,
                        help="filepath to save image to")
    parser.add_argument("-s", type=int, required=True,
                        help="Start time for image processing, in seconds")
    parser.add_argument("-e", type=int, required=True,
                        help="End time for image processing, in seconds")
    parser.add_argument("-f", type=int, default=24,
                        help="Sample every f frames (default 24)")
    args = parser.parse_args()
    average_video(args.i, args.o, args.s, args.e, args.f)
