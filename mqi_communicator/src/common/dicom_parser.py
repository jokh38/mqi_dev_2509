"""
DICOM parser utility for extracting beam information from RTPLAN files.

This module provides functionality to parse DICOM RTPLAN files and extract
relevant beam information for MOQUI TPS simulation setup.
"""

import pydicom
from pydicom.errors import InvalidDicomError
from typing import Dict, List, Any
from pathlib import Path

from src.common.structured_logging import get_structured_logger, LogContext

logger = get_structured_logger(__name__)


def get_plan_info(file_path: str) -> Dict[str, Any]:
    """
    Parses a DICOM RTPLAN file to extract beam information.
    
    Args:
        file_path: Path to the DICOM RTPLAN file
        
    Returns:
        Dictionary containing:
        - patient_id: Patient ID
        - patient_name: Patient name
        - plan_label: RT Plan label
        - beams: List of beam information dictionaries
        
    Raises:
        ValueError: If file is not a valid DICOM RTPLAN file
        FileNotFoundError: If file does not exist
    """
    file_path = Path(file_path)
    
    if not file_path.exists():
        error_msg = f"DICOM file not found: {file_path}"
        logger.error(
            error_msg,
            context=LogContext(
                operation="dicom_parsing",
                extra_data={"file_path": str(file_path)}
            ).to_dict()
        )
        raise FileNotFoundError(error_msg)
    
    try:
        logger.info(
            "Reading DICOM RTPLAN file",
            context=LogContext(
                operation="dicom_parsing",
                extra_data={"file_path": str(file_path)}
            ).to_dict()
        )
        
        ds = pydicom.dcmread(file_path, force=True)
    except InvalidDicomError as e:
        error_msg = f"Error reading DICOM file: {e}"
        logger.error_with_exception(
            "Invalid DICOM file",
            e,
            context=LogContext(
                operation="dicom_parsing",
                extra_data={"file_path": str(file_path)}
            ).to_dict()
        )
        raise ValueError(error_msg) from e

    rt_plan_data = {
        "patient_id": ds.get("PatientID", "N/A"),
        "patient_name": str(ds.get("PatientName", "N/A")),
        "plan_label": ds.get("RTPlanLabel", "N/A"),
        "beams": []
    }

    if ds.get("Modality") != "RTPLAN":
        error_msg = f"Error: DICOM file is not an RTPLAN. Modality is '{ds.get('Modality')}'."
        logger.error(
            error_msg,
            context=LogContext(
                operation="dicom_parsing",
                extra_data={
                    "file_path": str(file_path),
                    "modality": ds.get("Modality")
                }
            ).to_dict()
        )
        raise ValueError(error_msg)

    if not hasattr(ds, 'IonBeamSequence') or not ds.IonBeamSequence:
        logger.warning(
            "No IonBeamSequence found in RTPLAN",
            context=LogContext(
                operation="dicom_parsing",
                extra_data={"file_path": str(file_path)}
            ).to_dict()
        )
        return rt_plan_data

    logger.info(
        f"Found {len(ds.IonBeamSequence)} beams in RTPLAN",
        context=LogContext(
            operation="dicom_parsing",
            extra_data={
                "file_path": str(file_path),
                "beam_count": len(ds.IonBeamSequence)
            }
        ).to_dict()
    )

    for i, beam_ds in enumerate(ds.IonBeamSequence):
        beam_description = getattr(beam_ds, 'BeamDescription', '')
        beam_name = getattr(beam_ds, 'BeamName', '')

        if beam_description == "Site Setup" or beam_name == "SETUP":
            logger.info(
                f"Skipping beam {i+1}: {beam_name} (Site Setup or SETUP beam)",
                context=LogContext(
                    operation="dicom_parsing",
                    extra_data={
                        "beam_index": i+1,
                        "beam_name": beam_name,
                        "beam_description": beam_description
                    }
                ).to_dict()
            )
            continue

        beam_data = {}
        try:
            beam_data["beam_name"] = beam_ds.BeamName
        except AttributeError:
            beam_data["beam_name"] = f"Beam_{i+1}_Unnamed"
            logger.warning(
                f"Beam {i+1} has no BeamName, using generated name",
                context=LogContext(
                    operation="dicom_parsing",
                    extra_data={"beam_index": i+1}
                ).to_dict()
            )

        beam_data["snout_position"] = None  # Placeholder
        beam_data["has_range_shifter"] = "RangeShifterSequence" in beam_ds and bool(beam_ds.RangeShifterSequence)
        beam_data["energy_layers"] = []

        try:
            beam_data["treatment_machine_name"] = beam_ds.TreatmentMachineName
        except AttributeError:
            beam_data["treatment_machine_name"] = None
            logger.warning(
                f"Beam {i+1} has no TreatmentMachineName",
                context=LogContext(
                    operation="dicom_parsing",
                    extra_data={"beam_index": i+1, "beam_name": beam_data["beam_name"]}
                ).to_dict()
            )
        
        # Extract other relevant data from control points if necessary
        # For example, gantry angle from the first control point
        if hasattr(beam_ds, 'IonControlPointSequence') and beam_ds.IonControlPointSequence:
            first_cp = beam_ds.IonControlPointSequence[0]
            beam_data['gantry_angle'] = first_cp.get('GantryAngle', 0.0)
            logger.debug(
                f"Extracted gantry angle for beam {i+1}: {beam_data['gantry_angle']}",
                context=LogContext(
                    operation="dicom_parsing",
                    extra_data={
                        "beam_index": i+1,
                        "beam_name": beam_data["beam_name"],
                        "gantry_angle": beam_data['gantry_angle']
                    }
                ).to_dict()
            )
        else:
            beam_data['gantry_angle'] = 0.0
            logger.warning(
                f"No IonControlPointSequence found for beam {i+1}, using default gantry angle",
                context=LogContext(
                    operation="dicom_parsing",
                    extra_data={"beam_index": i+1, "beam_name": beam_data["beam_name"]}
                ).to_dict()
            )

        rt_plan_data["beams"].append(beam_data)
        
        logger.info(
            f"Processed beam {i+1}: {beam_data['beam_name']}",
            context=LogContext(
                operation="dicom_parsing",
                extra_data={
                    "beam_index": i+1,
                    "beam_name": beam_data["beam_name"],
                    "gantry_angle": beam_data['gantry_angle'],
                    "has_range_shifter": beam_data["has_range_shifter"]
                }
            ).to_dict()
        )

    logger.info(
        f"Successfully parsed RTPLAN with {len(rt_plan_data['beams'])} treatment beams",
        context=LogContext(
            operation="dicom_parsing",
            extra_data={
                "file_path": str(file_path),
                "patient_id": rt_plan_data["patient_id"],
                "plan_label": rt_plan_data["plan_label"],
                "treatment_beam_count": len(rt_plan_data["beams"])
            }
        ).to_dict()
    )

    return rt_plan_data


def find_rtplan_file(case_path: str) -> str:
    """
    Find RTPLAN DICOM file in a case directory.
    
    Args:
        case_path: Path to the case directory
        
    Returns:
        Path to the first RTPLAN file found
        
    Raises:
        FileNotFoundError: If no RTPLAN file is found
    """
    case_dir = Path(case_path)
    
    if not case_dir.exists():
        error_msg = f"Case directory not found: {case_path}"
        logger.error(
            error_msg,
            context=LogContext(
                operation="rtplan_search",
                extra_data={"case_path": case_path}
            ).to_dict()
        )
        raise FileNotFoundError(error_msg)
    
    # Look for files matching RTPLAN patterns
    rtplan_patterns = ['RP.*.dcm', 'RTPLAN*.dcm', '*.dcm']
    
    for pattern in rtplan_patterns:
        rtplan_files = list(case_dir.glob(pattern))
        
        # Filter for actual RTPLAN files by checking modality
        for rtplan_file in rtplan_files:
            try:
                ds = pydicom.dcmread(rtplan_file, force=True)
                if ds.get("Modality") == "RTPLAN":
                    logger.info(
                        f"Found RTPLAN file: {rtplan_file.name}",
                        context=LogContext(
                            operation="rtplan_search",
                            extra_data={
                                "case_path": case_path,
                                "rtplan_file": str(rtplan_file)
                            }
                        ).to_dict()
                    )
                    return str(rtplan_file)
            except Exception as e:
                logger.debug(
                    f"Skipping file {rtplan_file.name}: {e}",
                    context=LogContext(
                        operation="rtplan_search",
                        extra_data={
                            "file_path": str(rtplan_file),
                            "error": str(e)
                        }
                    ).to_dict()
                )
                continue
    
    error_msg = f"No RTPLAN file found in case directory: {case_path}"
    logger.error(
        error_msg,
        context=LogContext(
            operation="rtplan_search",
            extra_data={"case_path": case_path}
        ).to_dict()
    )
    raise FileNotFoundError(error_msg)