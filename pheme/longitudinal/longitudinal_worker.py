from datetime import datetime
import logging
from time import time

from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.sql import and_, or_

from .select_or_insert import SelectOrInsert
from .stripXML import strip as stripXML
from .tables import AdmissionSource, SpecimenSource
from .tables import PerformingLab, LabFlag
from .tables import OrderNumber, ReferenceRange
from .tables import AssignedLocation, Race
from .tables import AdmitReason, ChiefComplaint, FluVaccine
from .tables import H1N1Vaccine, AdmissionO2sat
from .tables import AdmissionTemp, Pregnancy, Note
from .tables import LabResult, Location, Visit
from .tables import MessageProcessed, ServiceArea
from .tables import Disposition, VisitLabAssociation
from .tables import Diagnosis, VisitDiagnosisAssociation
from pheme.util.pg_access import AlchemyAccess
from pheme.warehouse.tables import ObservationData, HL7_Nte, FullMessage
from pheme.util.util import getDobDatetime, getYearDiff, inProduction
from pheme.util.util import none_safe_min as min
from pheme.util.util import none_safe_max as max


def pdb_hook():
    """Debugging hook for multi-processing

    Using multi-processing requires restoration of stdin/out
    Depending on where the hook is set, might consider reducing
    longitudinal_manager.NUM_PROCS to 1 to avoid confusion.

    """
    import pdb
    pdb.Pdb(stdin=open('/dev/stdin', 'r+'),
            stdout=open('/dev/stdout', 'r+')).set_trace()


class ClinicalInfo(object):
    """Base class for `clinical information` surrogates

    The derived surrogats are used to manage the extraction and
    deduplication of any clinical data.  This includes patient age,
    self-reported vaccination status, pregnancy, body temperature and
    oxygen saturation

    """
    def __init__(self, result, units):
        self.result = stripXML(result)
        self.units = units


def obr_index(hl7_obr_id, surrogate_lab_list):
    """Finds the index to the SurrogateLab with the hl7_obr_id

    Returns the first match found, raises KeyError otherwise

    """
    for lab, index in zip(surrogate_lab_list,
                          range(0, len(surrogate_lab_list))):
        if lab.hl7_obr_id == hl7_obr_id:
            return index

    raise KeyError("no lab with hl7_obr_id %d" % hl7_obr_id)


def obx_index(hl7_obx_id, surrogate_lab_list):
    """Finds the index to the SurrogateLab with the hl7_obx_id

    raises KeyError if no match, or multiple matches are found

    """
    match = None
    for lab, index in zip(surrogate_lab_list,
                          range(0, len(surrogate_lab_list))):
        if hl7_obx_id in lab.hl7_obx_ids:
            if match != None:
                raise KeyError("multiple labs with hl7_obx_id %d" %
                               hl7_obx_id)
            match = index
    if match is None:
        raise KeyError("no lab with hl7_obx_id %d" % hl7_obx_id)
    return match


class SurrogatePatientAge(ClinicalInfo):
    def associate(self, visit):
        """Link this instance with the given visit """
        if not self.units == 'Years':
            # Current db schema only holds 'years'.  Skip storage if
            # in a different unit, allow _calculateAge to take over.
            return

        visit.visit.age = int(self.result)


class SurrogateInfluenzaVaccine(ClinicalInfo):
    def associate(self, visit):
        """Link this instance with the given visit """
        flu = FluVaccine(status=self.result)
        flu = visit.parent_worker.flu_vaccine_lock.fetch(flu)
        visit.visit.dim_flu_vaccine_pk = flu.pk


class SurrogateH1N1Vaccine(ClinicalInfo):
    def associate(self, visit):
        """Link this instance with the given visit """
        vac = H1N1Vaccine(status=self.result)
        vac = visit.parent_worker.h1n1_vaccine_lock.fetch(vac)
        visit.visit.dim_h1n1_vaccine_pk = vac.pk


class SurrogateO2Saturation(ClinicalInfo):
    def associate(self, visit):
        """Link this instance with the given visit """
        if not self.units in ['Percent',
                              'PercentOxygen[Volume Fraction Units]']:
            raise ValueError(self.units)

        # Occasionally the percentage comes in with a trailing '.'
        # which kills the int cast - chop if present
        if self.result.endswith('.'):
            self.result = self.result[:-1]

        sat = AdmissionO2sat(o2sat_percentage=int(self.result))
        sat = visit.parent_worker.admission_o2sat_lock.fetch(sat)
        visit.visit.dim_admission_o2sat_pk = sat.pk


class SurrogateBodyTemp(ClinicalInfo):
    def associate(self, visit):
        """Link this instance with the given visit """
        if not self.units == 'Degree Fahrenheit [Temperature]':
            raise ValueError(self.units)
        #Natasha has requested we limit the precision to one
        #decimal place:
        self.result = "%.1f" % float(self.result)
        temp = AdmissionTemp(degree_fahrenheit=self.result)
        temp = visit.parent_worker.admission_temp_lock.fetch(temp)
        visit.visit.dim_admission_temp_pk = temp.pk


class SurrogatePregnancy(ClinicalInfo):
    def associate(self, visit):
        """Link this instance with the given visit """

        # The pregnancy message uses a 'CE' data type OBX statement,
        # which translates to needing the 5.2 portion of the result
        # so grab the value between the first and second pipes
        segments = self.result.split('|')
        prego = Pregnancy(result=segments[1])
        prego = visit.parent_worker.pregnancy_lock.fetch(prego)
        visit.visit.dim_pregnancy_pk = prego.pk


class SurrogateChiefComplaint(ClinicalInfo):
    def associate(self, visit):
        """Link this instance with the given visit """
        cc = ChiefComplaint(chief_complaint=self.result)
        cc = visit.parent_worker.chief_complaint_lock.fetch(cc)
        visit.visit.dim_cc_pk = cc.pk


#TODO: add '43137-9'; Clinical Finding - CONDITION OF INTEREST PRESENT
#TODO: consider other message types for age - all other codes below
#      only come in on ORU^R01^ORU_R01 - calculated age does not.
clinical_codes_of_interest = {
    '8661-1': SurrogateChiefComplaint,
    '29553-5': SurrogatePatientAge,
    '46077-4': SurrogateInfluenzaVaccine,
    '29544-4': SurrogateH1N1Vaccine,
    '20564-1': SurrogateO2Saturation,
    '59408-5': SurrogateO2Saturation,
    '8310-5': SurrogateBodyTemp,
    '11449-6': SurrogatePregnancy,
    }


class SurrogateDiagnosis(object):
    """A stand-in for each diagnosis built up during deduplication

    Diagnosis details are split between two DAO objects, the Diagnosis
    itself (icd9 & description), and the assocation which besides
    defining the association between the visit and the diagnosis,
    contains status & dx_datetime.

    This class simplifies adding and merging previously bound
    diagnoses with any new ones found during the longitudinal
    process.

    Each instance is intended to be 'Set' friendly, making it easy to
    compare and deduplicate, i.e. they are `hashable` and therefore
    immutable.

    """
    def __init__(self, rank, icd9, description, status, dx_datetime):
        """Store all the values defining this diagnosis.

        NB - these are to be treated as immutable objects, as part of
        the `hashable` contract.  dx_datetime is not part of the
        unique dx definition.

        """
        self.rank = rank
        self.icd9 = icd9
        self.description = description
        self.status = status
        self.dx_datetime = dx_datetime
        # Mark this immutable object as complete
        self._initialized = True

    def __setattr__(self, name, value):
        if hasattr(self, '_initialized'):
            raise TypeError("immutable object can't be changed")
        else:
            object.__setattr__(self, name, value)

    def __delattr__(self, name):
        raise TypeError("immutable object can't be changed")

    def __hash__(self):
        """Calculate and return a hash value for the instance

        Only fields in the respective DAO objects query_fields lists
        should be considered.  Other fields such as dx_datetime would
        result in IntegrityErrors as they aren't included in defining
        a unique diagnosis.

        returns a hash value for this instance

        """
        if not hasattr(self, '_hashvalue'):
            hv = self.icd9.__hash__() +\
                 self.status.__hash__()
            object.__setattr__(self, '_hashvalue', hv)

        return self._hashvalue

    def __cmp__(self, other):
        return cmp(self.__hash__(), other.__hash__())


class SurrogateLab(object):
    """A stand-in for each lab result built up during deduplication

    Lab details are split between several DAO objects, the LabResult
    itself (test_code, test_text, coding, result, result_unit) and the
    VisitLabAssocation which besides defining the association between
    the visit and the lab, contains status, collection_datetime,
    report_datetime and a number of related dimensional table foreign
    keys pointing to a LabFlag, OrderNumber, ReferenceRange, Note,
    PerformingLab and SpecimenSource

    This class simplifies adding and merging previously bound labs
    with any new ones found during the longitudinal process.  Only
    fields in the respective DAO `query_fields` tuples are considered
    in the comparison methods - so the first `unique` value will be
    kept.  For example, a new `report_datetime` is not part of the
    unique diagnosis definition, so a second lab differing only on
    such a field will be ignored.

    Each instance is intended to be 'Set' friendly, making it easy to
    compare and deduplicate, i.e. they are `hashable` and therefore
    immutable - note the `append_result` exception.

    """
    MAX_RESULT_LEN = 500

    def __init__(self, test_code, test_text, coding, result, units,
                 status, lab_flag, specimen_source, performing_lab,
                 order_number, reference_range,
                 collection_datetime, report_datetime,
                 hl7_obr_id=None, hl7_obx_id=None):
        """Store all the values defining this lab result.

        these are to be treated as immutable objects, as part of
        the `hashable` contract.  `report_datetime` is not part of the
        unique lab definition, nor are the foreign key associations
        {specimen_source, performing_lab, order_number,
        reference_range, lab_flag, note}.

        The `append_result` method side steps the immuatable contract,
        but does raise an exception if results are appended AFTER a
        call to __hash__ has been made - so make sure the object is
        complete before doing any sorting or inserting in sorted
        container types.

        As notes are collected as a later step, and don't affect
        unique checks or sorting, the note property is also an
        exception to the immutable contract, and not part of this
        initialization.

        """
        self.test_code = test_code
        self.test_text = test_text
        self.coding = coding
        self.result = result
        self.units = units
        self.status = status
        self.lab_flag = lab_flag
        self.performing_lab = \
                            PerformingLab(local_code=performing_lab)\
                            if performing_lab else None
        self.specimen_source =\
                             SpecimenSource(source=specimen_source)\
                             if specimen_source else None
        self.order_number = \
                          OrderNumber(filler_order_no=order_number)\
                          if order_number else None
        self.reference_range = \
                             ReferenceRange(range=reference_range)\
                             if reference_range else None
        self.collection_datetime = collection_datetime
        self.report_datetime = report_datetime
        self.hl7_obr_id = hl7_obr_id
        self.hl7_obx_ids = [hl7_obx_id, ] if hl7_obx_id else None
        self.note = None
        # Mark this immutable object as complete
        self._initialized = True

    @classmethod
    def from_VisitLabAssociation(cls, vla):
        """SurrogateLab factory method from VisitLabAssociation"""
        ss = vla.specimen_source.source if \
             vla.dim_specimen_source_pk else None
        pl = vla.performing_lab.local_code if \
             vla.dim_performing_lab_pk else None
        on = vla.order_number.filler_order_no if \
             vla.dim_order_number_pk else None
        rr = vla.reference_range.range if \
             vla.dim_ref_range_pk else None

        return cls(test_code=vla.lab.test_code,
                   test_text=vla.lab.test_text,
                   coding=vla.lab.coding,
                   result=vla.lab.result,
                   units=vla.lab.result_unit,
                   status=vla.status,
                   collection_datetime=vla.collection_datetime,
                   report_datetime=vla.report_datetime,
                   specimen_source=ss,
                   performing_lab=pl,
                   order_number=on,
                   reference_range=rr,
                   lab_flag=vla.lab_flag,)

    def append_result(self, result, hl7_obx_id):
        """This method is an exception to the immutable object
        contract, allowing the user to continue to build up the result
        after object creation.

        NB - this may only be used prior to any calls to
        `self.__hash__`, so make sure the object is complete before
        adding to a sorted container.

        Note also, the database limit of MAX_RESULT_LEN character
        length for this field is adhered to here - dropping anything
        beyond the max result length in the bit bucket.

        :param result: the additional result to append to the result
                       thus far.
        :param hl7_obx_id: the datawarehouse hl7_obx_id the result
                       came from - necessary for potential note
                       associations
        """
        if hasattr(self, '_hashvalue'):
            raise TypeError("append_result can't be called after "\
                            "__hash__")

        id_list = self.hl7_obx_ids + [hl7_obx_id, ]
        object.__setattr__(self, 'hl7_obx_ids', id_list)

        if result is None:
            return

        if self.result is not None:
            new_result = (" ".join((self.result,
                                    result)))[:self.MAX_RESULT_LEN]
        else:
            new_result = result[:self.MAX_RESULT_LEN]
        object.__setattr__(self, 'result', new_result)

    def set_note(self, value):
        """Notes are looked up as a second step, and are not part of
        the unique contract, so this setter doesn't oblidge by the
        immutable contract.

        """
        if value is not None and len(value):
            note = Note(note=value)
            object.__setattr__(self, 'note', note)
        else:
            object.__setattr__(self, 'note', None)

    def __setattr__(self, name, value):
        if hasattr(self, '_initialized'):
            raise TypeError("immutable object can't be changed")
        else:
            if name == 'result' and value is not None:
                value = value[:self.MAX_RESULT_LEN]
            object.__setattr__(self, name, value)

    def __delattr__(self, name):
        raise TypeError("immutable object can't be changed")

    def __hash__(self):
        """Calculate and return a hash value for the instance

        Only fields in the respective DAO objects query_fields lists
        should be considered.  Other fields such as report_datetime
        would result in IntegrityErrors as they aren't included in
        defining a unique lab result.

        NB - we're overlooking at the moment that two identical
        labs that differ by one of the other dimensions (note,
        specimen_source, lab_flag, reference_range) won't create
        unique rows.

        returns a hash value for this instance

        """
        if not hasattr(self, '_hashvalue'):
            hv = self.test_code.__hash__() +\
                 self.test_text.__hash__() +\
                 self.coding.__hash__() +\
                 self.result.__hash__() +\
                 self.units.__hash__() +\
                 self.status.__hash__()
            object.__setattr__(self, '_hashvalue', hv)

        return self._hashvalue

    def __cmp__(self, other):
        return cmp(self.__hash__(), other.__hash__())


class SurrogateVisit(object):
    """ Surrogate visits built up during the longitudinal process

    During the process of generating the longitudinal visit for a
    particular visit_id, a surrogate visit for each defined patient
    class is used to gather the best data, and house the more
    complicated logic to determine what should be kept, updated and
    ignored.

    A parent_worker attribute is maintainted so the surrogate can
    access attributes of the owning longitudinal worker, such as the
    table locks needed when looking up existing records.

    """

    def __init__(self, parent_worker, visit):
        """Handles a number of tricky related values via properties

        :param parent_worker: The longitudinal_worker that
        instantiated this instance.  Used to reference contained
        locks, etc.

        :param visit: The visit instance representing the DBO

        """
        self.parent_worker = parent_worker
        self.visit = visit
        self._admission_source = None
        self._assigned_location = None
        self._admit_reason = None
        self._chief_complaint = None
        self._disposition = None
        self._location = None
        self._service_area = None
        self._diagnoses = set()
        self._labs = set()
        self._clinical_info = {}
        self._race = None

    def _get_admission_source(self):
        return self._admission_source

    def _set_admission_source(self, admission_source):
        """Stores the new admission_source provided

        Simply keeps the latest, provided it has a value

        :param admission_source: the `admission_source` (code)
        directly from the HL7 message (PV1.14.1).  These are really
        lookup values, the description for each lives in the
        dim_admission_source table.

        """
        assert(admission_source and admission_source.strip())
        self._admission_source = AdmissionSource(pk=admission_source)

    admission_source = property(_get_admission_source, _set_admission_source)

    def associate_admission_source(self):
        """Bind the visit DAO with the admission_source, if set """
        if not self.admission_source:
            return
        a_s = self.parent_worker.admission_source_lock.\
             fetch(self.admission_source)
        self.visit.dim_admission_source_pk = a_s.pk

    def _get_assigned_location(self):
        return self._assigned_location

    def _set_assigned_location(self, location):
        """Stores the new assigned_location provided

        Simply keeps the latest, provided it has a value

        This method (and _set_service_area) also manage the
        'ever_in_icu' logic, as the qualifying assigned_location may
        chanage.  If we ever see a qualifying value, set ever_in_icu
        to True.  It's default value of False handles the obvious, and
        don't set it back false to overwrite what was possibly seen.

        :param assigned_location: the `assigned_patient_location`
        directly from the HL7 message (PV1.3.1).

        """
        assert(location and location.strip())
        if location.endswith('ICU') or location.endswith('ACU') or \
               location in ('ACUI',):
            self.visit.ever_in_icu = True
        self._assigned_location = AssignedLocation(location=location)

    assigned_location = property(_get_assigned_location,
                                 _set_assigned_location)

    def establish_associations(self):
        """Establish any associations available with this visit

        During the longitudinal process, many attributes on this
        surrogate visit class were set.  Those all need to be
        associated, typically by setting a foreign key value on the
        visit DAO instance.

        A number of 'associate_*' methods are defined for this class.
        This method looks up all callable attributes starting with the
        'associate' string, and calls them in turn.

        We also keep track of any 'related changes', that is, changes
        to the visit, that aren't made on the visit DAO itself, as a
        timestamp is kept current for any changes made.  The associate
        tables are such an example, where any new rows in an associate
        table including this visit's foreign key would count as a
        change on this instance.  NB - it is the obligation of the
        associate methods to return True in such a case.

        returns True if any changes were made to the visit object that
        aren't done to the visit DOA itself.

        """
        changed = []
        for attr in dir(self):
            if attr.startswith('associate'):
                method = getattr(self, attr)
                if callable(method):
                    changed.append(method())
        return any(changed)

    def associate_assigned_location(self):
        """Bind the visit DAO with the assigned_location, if set """
        if not self.assigned_location:
            return
        al = self.parent_worker.assigned_location_lock.\
             fetch(self.assigned_location)
        self.visit.dim_assigned_location_pk = al.pk

    def _get_admit_reason(self):
        return self._admit_reason

    def _set_admit_reason(self, admit_reason):
        """Stores the new admit_reason provided

        Simply keeps the latest, provided it has a value

        :param admit_reason: the `admit_reason` (code) directly from the
        HL7 message (PV2.3.2 or PV2.3.5).

        """
        assert(admit_reason and admit_reason.strip())
        self._admit_reason = AdmitReason(admit_reason=admit_reason)

    admit_reason = property(_get_admit_reason, _set_admit_reason)

    def associate_admit_reason(self):
        """Bind the visit DAO with the admit_reason, if set """
        if not self.admit_reason:
            return
        ar = self.parent_worker.admit_reason_lock.\
             fetch(self.admit_reason)
        self.visit.dim_ar_pk = ar.pk

    def _get_chief_complaint(self):
        return self._chief_complaint

    def _set_chief_complaint(self, chief_complaint):
        """Stores the new chief_complaint provided

        Simply keeps the latest, provided it has a value

        :param chief_complaint: the `chief_complaint` (code) directly from the
        HL7 message (PV2.3.2 or PV2.3.5).

        """
        assert(chief_complaint and chief_complaint.strip())
        self._chief_complaint = ChiefComplaint(chief_complaint=chief_complaint)

    chief_complaint = property(_get_chief_complaint, _set_chief_complaint)

    def associate_chief_complaint(self):
        """Bind the visit DAO with the chief_complaint, if set """
        if not self.chief_complaint:
            return
        cc = self.parent_worker.chief_complaint_lock.\
             fetch(self.chief_complaint)
        self.visit.dim_cc_pk = cc.pk

    def add_clinical_info(self, test_code, result, units):
        """Add clinical info to the visit

        There are a number of LOINC codes considered to be of
        interest, generally with their own dimension table in the
        database for persistance.  This SurrogateVisit instance
        maintains a dictionary of all the clinical info found during
        the longitudinal process.  Keyed by the LOINC code, with an
        instance of `ClinicalInfo` for each one found.

        If there already exists an entry for the LOINC code for this
        SurrogateVisit, ignore the new data - no updates kept for
        clinical information.

        """
        if test_code in self._clinical_info:
            return

        if result is None or len(result.strip()) == 0:
            return

        klass = clinical_codes_of_interest.get(test_code)
        self._clinical_info[test_code] = klass(result, units)

    def associate_clinical_info(self):
        """Create associations for new clinical info with the visit

        Each value in the `self._clinical_info` dictionary points to
        an instance of a class knowing how to link itself.

        """
        for ci in self._clinical_info.values():
            ci.associate(self)

    def _get_diagnoses(self):
        return self._diagnoses
    diagnoses = property(_get_diagnoses)

    def add_diagnosis(self, rank, icd9, description, status,
                      dx_datetime):
        """Stores the new diagnosis provided

        Add this diagnosis to the list, unless we already appear to
        have the same (icd9, description, status).  NB, the schema
        design breaks status & dx_datetime apart from icd9 & description.

        NB - self._diagnoses is a list of tuples containing:
        (Diagnosis, status, dx_datetime)

        :param diagnosis: the `diagnosis` (code) directly from the
                          HL7 message (PV1.36).

        """
        assert(icd9 and icd9.strip())
        # use a set to control duplicate entries
        self._diagnoses.add(SurrogateDiagnosis(rank=rank,
                                               icd9=icd9,
                                               description=description,
                                               status=status,
                                               dx_datetime=dx_datetime))

    def associate_diagnoses(self):
        """Bind any new diagnoses with the visit

        Load in any existing diagnoses associations, add in only new
        ones that didn't previously exist.

        returns True if any new associations were persisted to the
        database.

        """
        if not self._diagnoses:
            return False

        # First load in any existing, to avoid adding duplicates
        existing = self.parent_worker.data_mart.session.\
                   query(VisitDiagnosisAssociation).\
                   filter(VisitDiagnosisAssociation.fact_visit_pk ==
                          self.visit.pk)

        existing_set = set()
        for d in existing:
            existing_set.add(SurrogateDiagnosis(
                rank=d.rank,
                icd9=d.dx.icd9,
                description=d.dx.description,
                status=d.status,
                dx_datetime=d.dx_datetime))

        new_ones = self._diagnoses - existing_set
        new_associations = []
        for diagnosis in new_ones:
            diag_part = Diagnosis(icd9=diagnosis.icd9,
                                  description=diagnosis.description)
            d = self.parent_worker.diagnosis_lock.fetch(diag_part)
            new_associations.append(VisitDiagnosisAssociation(\
                fact_visit_pk=self.visit.pk,
                dim_dx_pk=d.pk,
                rank=diagnosis.rank,
                status=diagnosis.status,
                dx_datetime=diagnosis.dx_datetime))

        self.parent_worker.data_mart.session.add_all(new_associations)
        self.parent_worker.data_mart.session.commit()
        if new_associations:
            return True
        return False

    def _get_disposition(self):
        return self._disposition

    def _set_disposition(self, disposition):
        """Stores the new disposition provided

        Simply keeps the latest, provided it has a value

        :param disposition: the `disposition` (code) directly from the
                            HL7 message (PV1.36).

        """
        assert(disposition and disposition.strip())
        self._disposition = Disposition(code=disposition)

    disposition = property(_get_disposition, _set_disposition)

    def associate_disposition(self):
        """Bind the visit DAO with the disposition, if set """
        if not self.disposition:
            return
        d = self.parent_worker.disposition_lock.fetch(self.disposition)
        self.visit.dim_disposition_pk = d.code

    def _get_labs(self):
        return self._labs

    def _set_labs(self, labs):
        """Stores the list of new labs provided

        Deduplicates the list of labs, and hangs onto for
        association.  It is expected this method will be called zero
        or one time per SurrogateVisit - an exception is raised if
        called when labs have already been set.

        :param labs: an ordered list of SurrogateLab instances

        """
        if self._labs:
            raise ValueError("labs already set")
        # as self._labs is a set, addition in order will control
        # duplicates and retain the ones we want.
        for lab in labs:
            self._labs.add(lab)

    labs = property(_get_labs, _set_labs)

    def associate_labs(self):
        """Bind any new labs with the visit

        Load in any existing lab associations, add in only new
        ones that didn't previously exist.

        returns True if any new associations were persisted to the
        database.

        """
        if not self._labs:
            return False

        # First load in any existing, to avoid adding duplicates
        existing = self.parent_worker.data_mart.session.\
                   query(VisitLabAssociation).\
                   filter(VisitLabAssociation.fact_visit_pk ==
                          self.visit.pk)

        existing_set = set()
        for e in existing:
            existing_set.add(SurrogateLab.from_VisitLabAssociation(e))

        new_ones = self._labs - existing_set
        new_associations = []
        for lab in new_ones:
            result_part = LabResult(test_code=lab.test_code,
                                    test_text=lab.test_text,
                                    coding=lab.coding,
                                    result=lab.result,
                                    result_unit=lab.units)
            r = self.parent_worker.lab_result_lock.fetch(result_part)
            if lab.lab_flag:
                lf = self.parent_worker.lab_flag_lock.\
                     fetch(lab.lab_flag)
                lf_pk = lf.pk
            else:
                lf_pk = None
            if lab.performing_lab:
                pl = self.parent_worker.performing_lab_lock.\
                     fetch(lab.performing_lab)
                pl_pk = pl.pk
            else:
                pl_pk = None
            if lab.specimen_source:
                ss = self.parent_worker.specimen_source_lock.\
                     fetch(lab.specimen_source)
                ss_pk = ss.pk
            else:
                ss_pk = None
            if lab.order_number:
                on = self.parent_worker.order_number_lock.\
                     fetch(lab.order_number)
                on_pk = on.pk
            else:
                on_pk = None
            if lab.reference_range:
                rr = self.parent_worker.reference_range_lock.\
                     fetch(lab.reference_range)
                rr_pk = rr.pk
            else:
                rr_pk = None
            if lab.note:
                note = self.parent_worker.note_lock.\
                     fetch(lab.note)
                note_pk = note.pk
            else:
                note_pk = None
            new_associations.append(VisitLabAssociation(\
                fact_visit_pk=self.visit.pk,
                dim_lab_result_pk=r.pk,
                status=lab.status,
                collection_datetime=lab.collection_datetime,
                report_datetime=lab.report_datetime,
                dim_lab_flag_pk=lf_pk,
                dim_specimen_source_pk=ss_pk,
                dim_performing_lab_pk=pl_pk,
                dim_order_number_pk=on_pk,
                dim_ref_range_pk=rr_pk,
                dim_note_pk=note_pk))

        self.parent_worker.data_mart.session.add_all(new_associations)
        self.parent_worker.data_mart.session.commit()
        if new_associations:
            return True
        return False

    def _get_location(self):
        return self._location

    def _set_location(self, location):
        """Stores the new location provided, letting the old drop in
        the bit bucket.

        NB - we don't update locations, as they tend to change, and
        the old and new don't necessarily have any relation.  Just
        keep the latest.

        :param location: a prepared `Location` instance, not
                         necessarily persisted.

        """
        self._location = location

    location = property(_get_location, _set_location)

    def associate_location(self):
        """Bind the visit DAO with the location, if set """
        if not self.location:
            return
        loc = self.parent_worker.location_lock.fetch(self.location)
        self.visit.dim_location_pk = loc.pk

    def _get_race(self):
        return self._race

    def _set_race(self, race):
        """Stores the new race/ethnicity provided, letting the old
        drop in the bit bucket.

        :param race: the race or ethnicity string from which ever
                     value in the HL/7 PID field was valid (PID.22.2
                     or PID.10.2)

        """
        self._race = Race(race=race)

    race = property(_get_race, _set_race)

    def associate_race(self):
        """Bind the visit DAO with the race, if set """
        if not self.race:
            return
        race = self.parent_worker.race_lock.fetch(self.race)
        self.visit.dim_race_pk = race.pk

    def _get_service_area(self):
        return self._service_area

    def _set_service_area(self, service_area):
        """Stores the new service_area provided

        Simply keeps the latest, provided it has a value

        This method (and _set_assigned_location) also manage the
        'ever_in_icu' logic, as the qualifying assigned_location may
        chanage.  If we ever see a qualifying value, set ever_in_icu
        to True.  It's default value of False handles the obvious, and
        don't set it back false to overwrite what was possibly seen.

        :param service_area: the `service_code` directly from the HL7
                             message (PV1.10.1).

        """
        assert(service_area and service_area.strip())
        if service_area in ('INT', 'PIN'):
            self.visit.ever_in_icu = True
        self._service_area = ServiceArea(area=service_area)

    service_area = property(_get_service_area, _set_service_area)

    def associate_service_area(self):
        """Bind the visit DAO with the service_area, if set """
        if not self.service_area:
            return
        sa = self.parent_worker.service_area_lock.fetch(self.service_area)
        self.visit.dim_service_area_pk = sa.pk


class ObxSequence(object):
    """Type for inconsistent sequence types in OBX-4.1

    The OBX sub-id (hl7_obx.sequence) can be None, integer or floating
    point.  Data type to store and handle comparison in calulating if
    it's time for an increment in NextLabState

    """
    def __init__(self, sequence=None):
        self._set_seq(sequence)

    def _get_seq(self):
        return self.__seq

    # Treat sequence as a read only property - don't expose setter
    sequence = property(_get_seq)

    def reset(self):
        self.__seq = None

    def _set_seq(self, sequence):
        this_sequence = sequence.strip() if sequence else None
        if this_sequence:
            dot_index = this_sequence.find('.')
            if dot_index > 0:
                self.__whole = int(this_sequence[0:dot_index])
                self.__frac = int(this_sequence[dot_index + 1:])
            else:
                self.__whole = int(this_sequence)
                self.__frac = None
        self.__seq = this_sequence

    def in_sequence_with(self, other):
        """Compare this with another - only supports same type

        This method checks to see if 'other' might be in_sequence_with
        'self'.  Two cases in which this will happen:

        * Both self and other have the same whole value, and the
          fractional part of other is greater than that of self
          (i.e. 1.1 followed by 1.2)
        * Both self and other have the same, non zero, fractional
          value, and the whole part of other is greater than that of
          self (i.e. 1.1 followed by 2.1)

        :param other: the other sequence to compare self to, does
        other look to be in sequence with self.

        returns True if other appears to be in sequence with self,
        False otherwise.

        """
        if not isinstance(other, ObxSequence):
            raise ValueError("comparison of non ObxSequence not "
                             "supported")
        result = False
        if self.__seq and other.__seq:
            # Look for 1.1 -> 1.2 case
            if self.__whole == other.__whole and\
                   self.__frac and other.__frac and\
                   self.__frac < other.__frac:
                result = True
            # Look for 1.1 -> 2.1 case
            elif self.__whole < other.__whole and\
                     self.__frac and other.__frac and\
                     self.__frac == other.__frac:
                result = True
        return result


class NextLabState(object):
    """State machine to determine when to start the next lab

    The task of breaking out individual labs as they come in is
    messy.  This class manages state info, implementing a number of
    transition methods to determine when a new lab should be
    generated, and when the previous needs more data.

    The logic for separation of labs includes:
    1. next obr
    2. next obx within an obr without a defined sequence
    3. next obx within an obr with a new significant value
       in the sequence, i.e. 1.1 followed by 2.1
    4. next obx within an obr with a non-increasing value
       in the sequence, i.e. 1.1 followed by 1

    """
    def __init__(self):
        """Reset internal state"""
        self.__active_index = 0
        self.__active_lab_set = False
        self.__last_sequence = ObxSequence()

    def _get_active_index(self):
        return self.__active_index

    def __bump_active_index(self):
        self.__active_index += 1
        self.__active_lab_set = False
        self.__last_sequence.reset()

    # Treat index as a read only property - don't expose setter
    index = property(_get_active_index)

    def transition_new_obr(self):
        """Call any time a new obr is found"""
        if self.__active_lab_set:
            self.__bump_active_index

    def transition_new_obx(self, sequence, code):
        """Call any time a new obx is found

        Determine if it's time to bump the active index.

        :param sequence: The OBX-4.1 sub-ID field.  Typically None,
          an integer or a 1.1 format float.
        :param code: The loinc or local code - if it changed since
          last known, it's bump time regardless of the sequence.

        """
        this_sequence = ObxSequence(sequence)
        if self.__active_lab_set:
            if self.__last_code != code:
                self.__bump_active_index()
            elif not self.__last_sequence.in_sequence_with(this_sequence):
                self.__bump_active_index()
        self.__last_sequence = this_sequence
        # Call here implies a new lab is being added
        self.__active_lab_set = True
        self.__last_code = code


def _preferred_lab_data(obr, obx):
    """Function to pick the best code, text & coding for lab

    A lab result consists of the observation request (OBR) and the
    observation result.  In both, there exists both a preferred
    coding system, such as LOINC, and an alternative, such as
    local.

    We prefer the OBX codes, if non null, and within prefer the
    standardized coding system over local.  If nothing is
    available in the OBX, default to the OBR, again preferring
    standarized codes.

    The evaluation is done on the codes themselves, returning the
    matching text and coding system.  Therefore a null text may be
    returned even if there was a defined text on a less favorable
    group, if the more favorable has a defined code.

    :param obr: Observation Request (HL7_Obr) for this lab
    :param obx: Observation Result (HL7_Obx) for this lab

    returns the list (preferred_code, preferred_text, coding)

    """
    # Using only the code to determine what's available,
    # start with OBX preferred coding and move on down
    if obx.observation_id:
        code = obx.observation_id
        text = obx.observation_text if obx.observation_text\
               else None
        coding = obx.coding if obx.coding else None
    elif obx.alt_id:
        code = obx.alt_id
        text = obx.alt_text if obx.alt_text else None
        coding = obx.alt_coding if obx.alt_coding else None
    elif obr.loinc_code:
        code = obr.loinc_code
        text = obr.loinc_text if obr.loinc_text else None
        coding = obr.coding if obr.coding else None
    elif obr.alt_code:
        code = obr.alt_code
        text = obr.alt_text if obr.alt_text else None
        coding = obr.alt_coding if obr.alt_coding else None
    else:
        raise ValueError("no valid codes found for OBX or OBR")

    return (code, text, coding)


def _preferred_lab_flag(obx):
    """Grabs the preferred lab flag data from the obx row

    Lab flags include an identifier, text and coding.  There is both a
    preferred and an alternate set of each.  There may also be none of
    the above defined in the source HL7 obx segment.

    Given the HL7_Obx row data, extract the best lab flag data
    available.  If no data is found, return None.  Otherwise, return
    the best `LabFlag` available

    """
    if not any((obx.abnorm_id, obx.abnorm_text, obx.alt_abnorm_id,
               obx.alt_abnorm_text)):
        return None

    code, text, coding = None, None, None
    if obx.abnorm_id or obx.abnorm_text:
        code = obx.abnorm_id
        text = obx.abnorm_text
        coding = obx.abnorm_coding
    else:
        code = obx.alt_abnorm_id
        text = obx.alt_abnorm_text
        coding = obx.alt_abnorm_coding

    return LabFlag(code=code, code_text=text, coding=coding)


class LongitudinalWorker(object):
    """ Deduplicate a visit.

    Does actual processing for a visit.  This class is designed to be
    run concurrently with any number of like workers, in a
    multi-process environment.  Multi-threaded proved to be a very
    expensive thrashing experiment due to python's GIL (global
    interpreter lock).  Running as seperate processes, we sidestep the
    GIL bottleneck.  The real win is that each process has its own
    database connection, so time spent waiting on the db gives the
    other processes time to execute.

    """

    def __init__(self, queue, procNumber, data_warehouse, data_mart,
                 table_locks={}, dbHost='localhost', dbUser=None,
                 dbPass=None, mart_port=5432, warehouse_port=5432,
                 verbosity=0):
        self.data_warehouse = AlchemyAccess(database=data_warehouse,
                                            port=warehouse_port,
                                            host=dbHost, user=dbUser,
                                            password=dbPass)
        self.data_mart = AlchemyAccess(database=data_mart,
                                       host=dbHost, port=mart_port,
                                       user=dbUser, password=dbPass)
        self.queue = queue
        self.name = 'worker-%d' % procNumber
        self.verbosity = verbosity

        # Instantiate a SelectOrInsert tool for each provided lock,
        # named for the table it's protecting.
        # See `longitudinal_manager` for nomenclature
        for table, lock in table_locks.items():
            setattr(self, table,
                    SelectOrInsert(lock, self.data_mart.session))

        if self.queue:
            logging.info("%s: launching", self.name)
            self.run()

    def run(self):
        while True:
            startTime = time()

            # Grab an available visit_id off the queue
            visit_id = self.queue.get()
            try:
                self.dedupVisit(visit_id)

                logging.debug("%s: Merged %s in %s seconds", self.name,
                              visit_id, time() - startTime)

                # Every 100 visits log what's left
                whats_left = self.queue.qsize()
                if whats_left and whats_left % 100 == 0:
                    logging.info("%d visits yet to process", whats_left)

            except IntegrityError, i:
                logging.exception("%s: CRITICAL IntegrityError "
                                  "caught on visit %s : %s",
                                  self.name, visit_id, i)
                # rollback the transaction - otherwise this worker is
                # left with a useless session
                logging.info("%s: Rolling back visit %s",
                             self.name, visit_id)
                self.data_mart.session.rollback()

            except OperationalError, i:
                logging.exception("%s: CRITICAL OperationalError "
                                  "caught on visit %s : %s",
                                  self.name, visit_id, i)
                # rollback the transaction - otherwise this worker is
                # left with a useless session
                logging.info("%s: Rolling back visit %s",
                             self.name, visit_id)
                self.data_mart.session.rollback()

            except Exception, e:
                logging.exception("%s: CRITICAL Exception caught on "\
                                  "visit %s : %s",
                                  self.name, visit_id, e)
                if not inProduction():
                    raise e
                else:
                    self.data_mart.session.rollback()
            finally:
                # Mark this one done in the queue regardless of
                # success so we don't hang the process - it doesn't
                # get marked done in the db unless it did complete, so
                # it'll continue to get picked up next run till the
                # error is addressed.
                self.queue.task_done()

                # Clean up if we appear to be done.
                if self.queue.empty():
                    self.tearDown()

    def tearDown(self):
        """tearDown this worker, free resources peacefully

        The manager should call this once the queue is empty so
        open connections can be peacefully shutdown.

        """
        self.data_warehouse.disconnect()
        self.data_mart.disconnect()
        logging.info("%s: tearing down", self.name)

    def _handle_new_visit(self, message):
        """Local helper to handle a new visit

        Adds the visit to self._surrogates keyed by patient_class.
        It is the callers responsibility to persist the object.

        :param message: The HL7 message being processed, evidently the
        first for this (visit_id, patient_class).

        returns the new visit, also set in self._surrogates[pc]

        """
        v = message.visit
        visit = Visit(visit_id=v.visit_id,
                      patient_class=v.patient_class,
                      patient_id=v.patient_id,
                      admit_datetime=v.admit_datetime,
                      first_message=message.message_datetime,
                      last_message=message.message_datetime,
                      dim_facility_pk=message.facility)

        self._set_surrogate(v.patient_class, visit)
        return visit

    def _commit_visit(self, visit, forceUpdate=False):
        """Persist the deduplicated visit if necessary

        Roundtrip is skipped unless forced or necessary.

        :param visit: The Visit with all the merged / updated and
        related values set.

        :param forceUpdate: Used when associated tables get updates,
        we maintain the last_updated value.

        """

        if forceUpdate or self.data_mart.session.\
               is_modified(visit, include_collections=True,
                           passive=True):
            visit.last_updated = datetime.now()
            self.data_mart.session.commit()
            logging.info("%s: commit merged ER visit %s with "\
                             "admit_datetime %s", self.name,
                         visit.visit_id, visit.admit_datetime)
        else:
            logging.debug("%s: skipped commit(), '%s' doesn't look "\
                              "dirty", self.name, visit.visit_id)

    def _new_labs(self, query):
        """Local helper used to filter and chunk labs

        Labs are messy coming in, Each OBR can have any number of OBX
        statments, which may or may not define new replies.  This
        method takes the prebuilt query and chunks up the labs using
        the rules within - essentially relying on the value of
        obx.sequence to define continuation or a new result (lab).

        Labs also have associated segments, such as specimen source
        (SPM) and notes (NTE).  The later of which can be split over
        multiple segments, and associated either with the OBR or OBX.

        The other task here is to filter out non lab data, such as
        clinical information.

        :param query: prepared sqlalchemy query to return the list of
          `ObservationData` objects, containing obx and associated obr
          messages for consideration

        returns list containing a SurrogateLab for each lab result
        defined.  No checks are done within to assure these labs
        haven't already been linked, but the order should be intact to
        assure the first in the list were the first defined.

        """
        new_labs = list()

        # Use a NextLabState instance to manage the new_labs index.
        # See `NextLabState` for increment logic.
        transition_tool = NextLabState()

        #last_sig, active_lab_index = 0, -1
        for observation in query:
            transition_tool.transition_new_obr()
            #active_lab_index += 1
            for obx in observation.obxes:
                # Prefer the obx values; fall back to obr
                best_code, best_text, coding =\
                           _preferred_lab_data(observation, obx)

                result = stripXML(obx.observation_result)

                transition_tool.transition_new_obx(sequence=obx.sequence,
                                                   code=best_code)
                if len(new_labs) == transition_tool.index:
                    # Dealing w/ new lab, populate what we know.
                    code = best_code
                    text = best_text
                    collection_dt = observation.observation_datetime
                    report_dt = observation.report_datetime
                    if observation.status == 'A':
                        assert(obx.result_status == 'A' or\
                               obx.result_status is None)
                    lab_flag = _preferred_lab_flag(obx)
                    new_labs.append(
                        SurrogateLab(
                            test_code=code,
                            test_text=text,
                            coding=coding,
                            result=result,
                            units=obx.units,
                            status=observation.status,
                            collection_datetime=collection_dt,
                            report_datetime=report_dt,
                            lab_flag=lab_flag,
                            specimen_source=observation.specimen_source,
                            performing_lab=obx.performing_lab_code,
                            order_number=observation.filler_order_no,
                            reference_range=obx.reference_range,
                            hl7_obr_id=observation.hl7_obr_id,
                            hl7_obx_id=obx.hl7_obx_id))
                else:
                    # Confirm we didn't walk off the end
                    assert(transition_tool.index == len(new_labs) - 1)

                    # Continuation of lab - concatinate this result
                    new_labs[transition_tool.index].append_result(
                        result=result, hl7_obx_id=obx.hl7_obx_id)

        #Now need to fetch and re-associate notes
        self._associate_notes(new_labs)
        return new_labs

    def _associate_notes(self, labs):
        """Helper to lookup and associate any available notes for labs

        Due to the nature of notes (HL7 NTE segments), the data
        warehouse associations are maintained by using either
        hl7_obr_ids or hl7_obx_ids as foreign keys.  This complexity
        is necessary as a single HL7 message containing a number of
        observation results may have notes associated with the
        observation request (HL7 OBR segment) and/or any number of the
        observation results (HL7 OBX segments), each of which may span
        multiple segments itself.

        This method queries the datawarehouse for any available note
        associations, and pushes the results back into the
        SurrogateLabs provided.

        :param labs: list of SurrogateLab objects potentially needing
          notes.  Modified if any related notes are found.

        """
        hl7_obr_ids = [lab.hl7_obr_id for lab in labs]
        hl7_obx_ids = [i for lab in labs for i in lab.hl7_obx_ids]
        sq = self.data_warehouse.session.query
        query = sq(HL7_Nte).\
                filter(or_(HL7_Nte.hl7_obr_id.in_(hl7_obr_ids),
                           HL7_Nte.hl7_obx_id.in_(hl7_obx_ids))).\
                           order_by(HL7_Nte.hl7_obr_id,
                                    HL7_Nte.hl7_obx_id,
                                    HL7_Nte.sequence_number)
        #Build up notes from potential set of segments, maintaining
        #same mapping index key as used in id_map
        found_notes = dict()
        for note_segment in query:
            if note_segment.hl7_obx_id is not None:
                index = obx_index(note_segment.hl7_obx_id, labs)
            else:
                index = obr_index(note_segment.hl7_obr_id, labs)

            if index in found_notes:
                found_notes[index].append(note_segment.note)
            else:
                found_notes[index] = [note_segment.note, ]

        #Push the note associations back into the labs
        for index, note_list in found_notes.items():
            note = ' '.join([n for n in note_list if n])
            if note:
                labs[index].set_note(note)

    def _add_observations(self, observation_messages):
        """Local helper to add related observations data to surrogates

        :param observation_messages: list of new messages for
        consideration

        """
        msh_ids = [m.hl7_msh_id for m in observation_messages]
        sq = self.data_warehouse.session.query
        # LOINC '43140-3' == "CLINICAL FINDING PRESENT" - not lab data
        # Remember SQL null handling is odd, loinc_code != 'x'
        # excludes undefined loinc_codes.
        query = sq(ObservationData).\
                filter(and_(ObservationData.hl7_msh_id.in_(msh_ids),
                            or_(ObservationData.loinc_code !=
                                '43140-3',
                                ObservationData.loinc_code == None)))

        new_labs = self._new_labs(query)
        if new_labs:
            # Add the new labs to _all_ surrogates, as labs don't
            # contain a patient class association
            for sv in self._surrogates.values():
                sv.labs = new_labs

    def _query_messages_to_merge(self, visit_id):
        """Local helper to obtain all the new visit info.

        - visit_id : The visit_id actively being deduplicated.

        This should return the FullMessage data for any messages new
        to this visit since last run.

        returns the messages oldest to newest so the most recent info
        'updates' what was previously known.

        """
        dmq = self.data_mart.session.query
        ids = dmq(MessageProcessed.hl7_msh_id).\
                  filter(and_(MessageProcessed.visit_id == visit_id,
                              MessageProcessed.processed_datetime ==
                              None)).all()
        msg_ids = [id[0] for id in ids]

        sq = self.data_warehouse.session.query
        return sq(FullMessage).\
               filter(FullMessage.hl7_msh_id.in_(msg_ids)).\
               order_by(FullMessage.message_datetime)

    def _calculateAge(self, visit):
        """Calculate the age for visit if not already defined

        Uses the dob and admit_datetime for approximate value in
        years, if the preferred method (see `SurrogatePatientAge`)
        didn't succeed.

        """
        if visit.age is not None:
            return
        if visit.dob is None:
            logging.debug("%s: DOB not defined for visit '%s', "\
                              "can't calculate age", self.name,
                          visit.visit_id)
            return
        if visit.admit_datetime is None:
            logging.warn("%s: admit_datetime not defined for visit "\
                              "'%s', can't calculate age", self.name,
                          visit.visit_id)
            return
        visit.age = getYearDiff(getDobDatetime(visit.dob),
                          visit.admit_datetime)

        # Look out for case where newborn arrives before the average
        # used (15th of month) in calculating the DOB from M/Y.
        if visit.age == -1:
            visit.age = 0

    def _get_surrogate(self, patient_class):
        return self._surrogates.get(patient_class)

    def _load_surrogates(self, visit_id):
        """load the existing visits for this visit_id

        Query the database for all patient classes on this visit_id.
        The results are stored in self._surrogates key'd by
        patient_class.

        :param visit_id: the visit_id to query for

        """
        self._surrogates = {}
        sq = self.data_mart.session.query
        query = sq(Visit).\
                filter(Visit.visit_id == visit_id)
        for v in query:
            self._surrogates[v.patient_class] =\
                                              SurrogateVisit(self, v)

    def _set_surrogate(self, patient_class, visit):
        """set new visits not found via `_load_surrogates`

        :param patient_class: The single character representing the
        patient class for the visit, i.e. 'E' for ER

        :param visit: The newly formed DBO instance

        """
        assert not self._get_surrogate(patient_class)
        self._surrogates[patient_class] = SurrogateVisit(self, visit)

    def dedupVisit(self, visit_id):
        """ Process a single visit_id - grab all associated data and
        merge any new info into the visit_state table.

        This may result in more than one row in the visit
        table, as each unique (visit_id, patient_class) is treated
        separately.

        """
        if None and visit_id.startswith('id to debug'):
            pdb_hook()

        # Load any existing longitudinal visits for this id.  (Likely
        # to need them all if they exist for observation connections).
        self._load_surrogates(visit_id)

        # Observation messages are dealt with after we have all the
        # respective patient_class visits built out - collect as we go.
        observation_messages = []
        clinical_messages = []
        query = self._query_messages_to_merge(visit_id)

        no_class_min_message_datetime = None
        no_class_max_message_datetime = None

        for message in query:
            if message.message_type == 'ORM^O01^ORM_O01':
                # Nothing of value at this time in order messages
                continue

            message_datetime = message.message_datetime

            pc = message.visit.patient_class
            if message.message_type == 'ORU^R01^ORU_R01':
                no_class_max_message_datetime = max(
                    message_datetime, no_class_max_message_datetime)
                no_class_min_message_datetime = min(
                    message_datetime, no_class_min_message_datetime)

                if pc not in ('E', 'I', 'O'):
                    observation_messages.append(message)
                else:
                    # This is a shortcut, using the lack of a patient
                    # class to determine if it's clinical data - saves
                    # the database hit.  We still associate all
                    # clinical_messages with all patient classes like
                    # observation_messages
                    clinical_messages.append(message)
                continue

            # Don't create a new visit (on patient class) if the pc is
            # 'U' (unknown).  If we only have one patient class, we
            # can safely assume it's the same visit - in any other
            # case, log and toss this one.
            if pc == 'U':
                if len(self._surrogates) == 1:
                    pc = self._surrogates.keys()[0]
                else:
                    logging.error("'U' patient class on message '%s' "
                                  "for visit '%s' which has multiple "
                                  "patient classes, don't know which "
                                  "to associate data with",
                                  message.message_control_id,
                                  visit_id)
                    continue
            sv = self._get_surrogate(pc)
            longitudinal_visit = sv.visit if sv else None
            if longitudinal_visit is None:
                longitudinal_visit = self._handle_new_visit(message)

            longitudinal_visit.first_message = min(
                message_datetime, longitudinal_visit.first_message)
            longitudinal_visit.last_message = max(
                message_datetime, longitudinal_visit.last_message)

            # Don't "update" with older messages
            if message_datetime < longitudinal_visit.last_message:
                logging.warn("skipping what looks like a stale, " +\
                             "duplicate message '%s' for visit '%s'",
                             message.message_control_id, visit_id)
                continue

            # Handle the columns with values in fact_visit not already
            # processed (via new visit or message times)
            for field in ('admit_datetime', 'discharge_datetime',
                          'gender', 'dob', 'disposition'):
                value = getattr(message.visit, field, None)
                b4 = getattr(longitudinal_visit, field, None)
                if value and value != b4:
                    setattr(longitudinal_visit, field, value)

            # Demographics
            demographic_values = dict([(field, getattr(message.visit,
                                                       field, None))
                           for field in ('zip', 'country', 'state',
                                         'county')])
            if any(demographic_values.values()):
                self._surrogates[pc].location =\
                                              Location(**demographic_values)

            a_s = getattr(message.visit, 'admission_source', None)
            if a_s:
                self._surrogates[pc].admission_source = a_s
            al = getattr(message.visit, 'assigned_patient_location', None)
            if al:
                self._surrogates[pc].assigned_location = al
            cc = getattr(message.visit, 'chief_complaint', None)
            if cc and cc.strip():
                self._surrogates[pc].admit_reason = cc
                self._surrogates[pc].chief_complaint = cc
            dis = getattr(message.visit, 'disposition', None)
            if dis:
                self._surrogates[pc].disposition = dis
            race = getattr(message.visit, 'race', None)
            if race:
                self._surrogates[pc].race = race
            sa = getattr(message.visit, 'service_code', None)
            if sa:
                self._surrogates[pc].service_area = sa
            for dx in message.dxes:
                # the HL/7 stream does include some blanks that are of
                # no value - skip if we don't at least have an icd9
                if dx.dx_code is None or len(dx.dx_code.strip()) == 0:
                    continue

                # dx_datetime is never populated in the HL/7 stream
                # agreed to use the message datetime as an approximation
                self._surrogates[pc].add_diagnosis(\
                    rank=dx.rank,
                    icd9=dx.dx_code,
                    description=dx.dx_description,
                    status=dx.dx_type,
                    dx_datetime=message_datetime)
            for obx in message.obxes:
                test_code = obx.observation_id
                if test_code in clinical_codes_of_interest:
                    self._surrogates[pc].add_clinical_info(
                        test_code=test_code,
                        result=obx.observation_result,
                        units=obx.units)

        # At this point, we must have an admit_datetime for every
        # longitudinal visit created above.  It turns out we
        # occasionally get a visit without a valid time - Mike
        # T. reports these are canceled visits.  If found, log and
        # move on.
        for pc, sv in self._surrogates.items():
            if sv.visit.admit_datetime is None:
                logging.warn("Visit %s : %s lacks the "
                             "required admit_datetime field",
                             visit_id, pc)
                # Have to mark it, or we'll keep retrying every time.
                update = """UPDATE internal_message_processed SET
                processed_datetime = '%s' WHERE processed_datetime IS
                NULL AND visit_id  = '%s' """ % (datetime.now(), visit_id)
                self.data_mart.engine.execute(update)
                return
            else:
                if sv.visit.pk is None:
                    self.data_mart.session.add(sv.visit)
                    self.data_mart.session.commit()
                    logging.debug("%s: new visit added '%s'", self.name,
                                  sv.visit.visit_id)

        # See if there is any new lab data to handle.  Lab data
        # doesn't contain patient class info and must be associated
        # with all visits regardless of patient class
        if observation_messages and self._surrogates:
            if None and visit_id.startswith('id to debug'):
                pdb_hook()
            self._add_observations(observation_messages)

        # The patient class in the observation messages doesn't appear
        # to be reliable.  Agreed to associate any clinical data with
        # all matching visit_ids regardless of patient class
        if clinical_messages and self._surrogates:
            for message in clinical_messages:
                for obx in message.obxes:
                    test_code = obx.observation_id
                    if test_code in clinical_codes_of_interest:
                        for sv in self._surrogates.values():
                            sv.add_clinical_info(
                                test_code=test_code,
                                result=obx.observation_result,
                                units=obx.units)

        # Commit changes if needed.
        for sv in self._surrogates.values():
            # First, associate any dimensions created above
            related_changes = sv.establish_associations()

            # adjust first/last datetimes if we picked up one w/o a pc
            sv.visit.first_message = min(sv.visit.first_message,
                                         no_class_min_message_datetime)
            sv.visit.last_message = max(sv.visit.last_message,
                                        no_class_max_message_datetime)
            self._calculateAge(sv.visit)  # in case it wasn't provided
            self._commit_visit(sv.visit, related_changes)

        # Mark those rows as processed
        self.data_mart.engine.execute("""UPDATE internal_message_processed SET
        processed_datetime = '%s' WHERE processed_datetime IS NULL AND visit_id
        = '%s' """ % (datetime.now(), visit_id))
