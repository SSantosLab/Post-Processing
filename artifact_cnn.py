"""DECam CNN-based Difference Imaging Artifact Detection.

Written by Robert Morgan and Adam Shandonay.

See https://arxiv.org/abs/2106.11315v1 for details.
"""

import numpy as np
from scipy.stats import mode
import torch

import cnn_utils 

## CNN Version ##
__version__ = 1.0


class StampEvaluator:
    """An object to score difference imaging stamps.

    Operates on an array of stamps with dimensions (N, 3, 51, 51) where the
    first dimension is the index of the detection, the second axis is (srch,
    temp, diff), the third axis is height, and the final axis is width.

    Usage:
    >>> evaluator = StampEvaluator(stamp_array)
    >>> scores = evaluator.run()
    """

    def __init__(
            self, stamps: np.ndarray, psf_array: np.ndarray = None,
            flux_array: np.ndarray = None, fluxerr_array: np.ndarray = None,
            masking_box_width: int = 14, model_dir = '.'):
        """Instnatiates a StampEvaluator object.

        Stores stamps, thresholds, and trained CNNs as class attributes.

        Args:
          stamps (np.ndarray): A 4-dimensional array of stamps with axes
            (index, stamp type, height, width).
          psf_array (np.ndarray): A 1-dimensional array of psfs for each
            detection in stamps.
          flux_array (np.ndarray): A 1-dimensional array of fluxcals for each
            detection in stamps.
          fluxerr_array (np.ndarray): A 1-dimensional array of fluxcalerrs for
            each detection in stamps.
          masking_box_width (int): Side length of box to check for masking
            in diff images.
          model_dir (str): Path to models, default is CWD.
        """
        self.stamps = stamps
        self.psf = psf_array
        self.flux = flux_array
        self.fluxerr = fluxerr_array
        self.snr_threshold = 3.76
        self.flux_threshold = 13.86
        self.cnn1_threshold = 0.5027928
        self.masking_box_width = masking_box_width
        self.model_dir = model_dir
        self.load_cnns()

    def run(self):
        """Returns stamp scores.

        Converts the stamps to a PyTorch Dataset, applies preprocessing
        filters to remove easy artifacts, applies 2 CNNs to the stamps, and 
        produces a final output probability for each detection.
        """
        dataset = self.make_dataset()
        preprocess_mask = self.preprocess()
        cnn1_scores = self.run_cnn(dataset, 'cnn1')
        cnn2_scores = self.run_cnn(dataset, 'cnn2')
        return self.score_stamps(preprocess_mask, cnn1_scores, cnn2_scores)

    def load_cnns(self):
        """Instantiates CNNs and stores them as class attributes."""
        self.cnn1 = cnn_utils.CNN()
        self.cnn1.load_state_dict(torch.load(f'{self.model_dir}/model1.pt'))
        self.cnn1.eval()
        self.cnn2 = cnn_utils.CNN()
        self.cnn2.load_state_dict(torch.load(f'{self.model_dir}/model2.pt'))
        self.cnn2.eval()

    def make_dataset(self) -> torch.utils.data.Dataset:
        """Convert input stamp array to PyTorch Dataset.

        Returns:
          An ArtifactDataset object made from the stamps."""
        transform = cnn_utils.ToTensor()
        return cnn_utils.ArtifactDataset(self.stamps, transform)

    def _snr_preprocessing(self):
        """Apply a SNR threshold to the images."""
        if self.flux is None or self.fluxerr is None:
            return np.ones(len(self.stamps), dtype=bool)

        snrs = self.flux.astype(float) / self.fluxerr.astype(float)
        return snrs > self.snr_threshold

    def _flux_preprocessing(self):
        """Apply a flux threshold to the images."""

        if self.psf is None:
            return np.ones(len(self.stamps), dtype=bool)
        
        def gaussian2D(distance_to_center, sigma):
            return (1 / (sigma ** 2 * 2 * np.pi) *
                    np.exp(-0.5 * (distance_to_center / sigma) ** 2))

        psfs = self.psf.astype(float)
        srch_images = self.stamps[:,0,:,:]
        psf_in_px = psfs[:,np.newaxis,np.newaxis] / 0.263 / 2.3548
        yy, xx = np.indices(srch_images[0].shape)
        center_x, center_y = 25, 25
        distance_to_center = np.sqrt(
            (yy - center_y)**2 + (xx - center_x)**2)[np.newaxis,:]
        distance_to_center = np.vstack(len(srch_images)*[distance_to_center])
        psf_weights = gaussian2D(distance_to_center, psf_in_px)
        backgrounds = np.median(
            srch_images, axis=(-1,-2))[:,np.newaxis,np.newaxis]
        center_fluxes = np.sum(
            (srch_images - backgrounds) * psf_weights, axis=(-1,-2))
        return center_fluxes > self.flux_threshold
    
    def _mask_preprocessing(self):
        """True if there is no masking in center of diff."""
        center = np.shape(self.stamps[0])[-1] // 2
        box_width = self.masking_box_width // 2
        center_boxes = self.stamps[:, 2,
            center - box_width : center + box_width, 
            center - box_width : center + box_width]

        # Get the difference image mask values
        mask_vals = np.median(
            self.stamps[:,2,:,:], axis=(-1, -2)).astype(int)

        # Get tht most common pixel values with a loop
        num = len(center_boxes)
        modes = np.array(
            [mode(center_boxes[i], axis=None)[0] for i in range(num)])

        # Check when the mask value is the most common
        return modes.flatten() != mask_vals
        
    def preprocess(self):
        """Run stamps through preprocessing functions.

        Args:
          dataset (torch.utils.data.Dataset): The output of make_dataset().
        
        Returns:
          Boolean array True where a detection passes preprocessing.
        """
        snr_mask = self._snr_preprocessing()
        flux_mask = self._flux_preprocessing()
        masking_mask = self._mask_preprocessing()
        return snr_mask & flux_mask & masking_mask
        
    def run_cnn(
            self,
            dataset: torch.utils.data.Dataset,
            cnn_attribute_name: str) -> np.ndarray:
        """Obtain scores from a CNN.

        Args:
          dataset (torch.utils.data.Dataset): The output of make_dataset().
          cnn_attribute_name (str): The name of the CNN to use.

        Returns:
          A numpy array of scores from the CNN.
        """
        cnn = getattr(self, cnn_attribute_name)
        scores = cnn(dataset[:]['image']).data.numpy()[:,0]
        scaled_scores = self._scale_scores(scores, cnn_attribute_name)
        return scaled_scores

    @staticmethod
    def _scale_scores(
            scores: np.ndarray, cnn_attribute_name: str) -> np.ndarray:
        """
        Scale network outputs to the interval [0, 1] depending on the network.

        Both networks scale the outputs to the interval [0, 1] using a min
        max scaling determined during the training. Network 1 applies an
        additional spreading of the outputs near central probabilities.

        Args:
          scores (np.ndarray): Array of single-class probabilities from net.
          cnn_attribute_name (str): The name of the CNN to use.

        Returns:
          An array of scaled scores.
        """
        scaling_vals = {
            'cnn1': (-69.225204, 69.225204),
            'cnn2': (-6.5211115, 6.5196385)}

        # Calibrate the network outputs using the min and max output found
        # during the training calibration
        net_min, net_max = scaling_vals[cnn_attribute_name]
        new_scores = np.where(scores > net_min, scores, net_min)
        new_scores -= net_min
        new_scores = np.where(new_scores < net_max, new_scores, net_max)
        new_scores /= net_max

        if cnn_attribute_name == 'cnn2':
            return new_scores
        
        # Spread the CNN1 scores to better fill the interval [0, 1] in the same
        # way that we did it for the network training. This step is necessary
        # so that the cnn1_threshold is correct for the network outputs.
        floored_scores = np.where(new_scores > 0.3, new_scores, 0.3)
        truncated_scores = np.where(
            floored_scores < 0.58, floored_scores, 0.58)
        scaled_scores = (truncated_scores - 0.3) / (0.58 - 0.3)
        return scaled_scores

    def score_stamps(
            self,
            preprocess_mask: np.ndarray,
            cnn1_scores: np.ndarray,
            cnn2_scores: np.ndarray) -> np.ndarray:
        """Combine preprocessing and CNN scores into one score.

        Detections that do not pass preprocessing are given a score of 0.
        Detections that pass preprocessing but not cnn1 are given a score
        between 0 and 0.1 based on their raw cnn1 score. Detections passing
        preprocessing and cnn1 are given their cnn2 score, but a floor is
        applied at 0.1.

        Args:
          preprocess_mask (np.ndarray): Boolean array for passing preproces.
          cnn1_scores (np.ndarray): Scores from CNN 1.
          cnn2_scores (np.ndarray): Scores from CNN 2.
        
        Returns:
          Combined scores of preprocessing and CNNs in a single array.
        """
        scaled_cnn1_scores = cnn1_scores / 10.0
        floored_cnn2_scores = np.where(cnn2_scores < 0.1, 0.1, cnn2_scores)
        return np.where(
            preprocess_mask,
            np.where(
                cnn1_scores < self.cnn1_threshold,
                scaled_cnn1_scores,
                floored_cnn2_scores),
            0.0)
