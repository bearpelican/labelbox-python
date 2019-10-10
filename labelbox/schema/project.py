from collections import namedtuple
from datetime import datetime, timezone
import json
import logging
import time

from labelbox import utils
from labelbox.exceptions import InvalidQueryError
from labelbox.orm import query
from labelbox.orm.db_object import DbObject, Updateable, Deletable
from labelbox.orm.model import Entity, Field, Relationship
from labelbox.pagination import PaginatedCollection


logger = logging.getLogger(__name__)


class Project(DbObject, Updateable, Deletable):
    """ A Project is a container that includes a labeling frontend, an ontology,
    datasets and labels.
    """
    name = Field.String("name")
    description = Field.String("description")
    updated_at = Field.DateTime("updated_at")
    created_at = Field.DateTime("created_at")
    setup_complete = Field.DateTime("setup_complete")
    last_activity_time = Field.DateTime("last_activity_time")
    auto_audit_number_of_labels = Field.Int("auto_audit_number_of_labels")
    auto_audit_percentage = Field.Float("auto_audit_percentage")

    # Relationships
    datasets = Relationship.ToMany("Dataset", True)
    created_by = Relationship.ToOne("User", False, "created_by")
    organization = Relationship.ToOne("Organization", False)
    reviews = Relationship.ToMany("Review", True)
    labeling_frontend = Relationship.ToOne("LabelingFrontend")
    labeling_frontend_options = Relationship.ToMany(
        "LabelingFrontendOptions", False, "labeling_frontend_options")
    labeling_parameter_overrides = Relationship.ToMany(
        "LabelingParameterOverride", False, "labeling_parameter_overrides")
    webhooks = Relationship.ToMany("Webhook", False)
    benchmarks = Relationship.ToMany("Benchmark", False)
    active_prediction_model = Relationship.ToOne("PredictionModel", False,
                                                 "active_prediction_model")
    predictions = Relationship.ToMany("Prediction", False)

    def create_label(self, **kwargs):
        """ Creates a label on this project.
        Kwargs:
            Label attributes. At the minimum the label `DataRow`
            and `Type` relationships and `label`, `seconds_to_label`
            fields.
        """
        # Copy-paste of Client._create code so we can inject
        # a connection to Type. Type objects are on their way to being
        # deprecated and we don't want the Py client lib user to know
        # about them. At the same time they're connected to a Label at
        # label creation in a non-standard way (connect via name).

        Label = Entity.named("Label")
        kwargs[Label.project] = self
        data = {Label.attribute(attr) if isinstance(attr, str) else attr:
                value.uid if isinstance(value, DbObject) else value
                for attr, value in kwargs.items()}

        query_str, params = query.create(Label, data)
        # Inject connection to Type
        query_str = query_str.replace("data: {",
                                      "data: {type: {connect: {name: \"Any\"}} ")
        res = self.client.execute(query_str, params)
        res = res["data"]["createLabel"]
        return Label(self.client, res)

    def labels(self, datasets=None, order_by=None):
        Label = Entity.named("Label")

        if datasets is not None:
            where = " where:{dataRow: {dataset: {id_in: [%s]}}}" % ", ".join(
                '"%s"' % dataset.uid for dataset in datasets)
        else:
            where = ""

        if order_by is not None:
            query.check_order_by_clause(Label, order_by)
            order_by_str = "orderBy: %s_%s" % (
                order_by[0].graphql_name, order_by[1].name.upper())
        else:
            order_by_str = ""

        query_str = """query GetProjectLabelsPyApi($project_id: ID!)
            {project (where: {id: $project_id})
                {labels (skip: %%d first: %%d%s%s) {%s}}}""" % (
            where, order_by_str, query.results_query_part(Label))

        return PaginatedCollection(
            self.client, query_str, {"project_id": self.uid},
            ["project", "labels"], Label)

    def export_labels(self, timeout_seconds=60):
        """ Calls the server-side Label exporting that generates a JSON
        payload, and returns the URL to that payload.
        Args:
            timeout_seconds (float): Max waiting time, in seconds.
        Return:
            URL of the data file with this Project's labels. If the server
                didn't generate during the `timeout_seconds` period, None
                is returned.
        """
        sleep_time = 2
        id_param = "projectId"
        query_str = """mutation GetLabelExportUrlPyApi($%s: ID!)
            {exportLabels(data:{projectId: $%s }) {downloadUrl createdAt shouldPoll} }
        """ %  (id_param, id_param)

        while True:
            res = self.client.execute(query_str, {id_param: self.uid})[
                "data"]["exportLabels"]
            if not res["shouldPoll"]:
                return res["downloadUrl"]

            timeout_seconds -= sleep_time
            if timeout_seconds <= 0:
                return None

            logger.debug("Project '%s' label export, waiting for server...",
                         self.uid)
            time.sleep(sleep_time)

    def labeler_performance(self):
        """ Returns the labeler performances for this Project.
        Returns:
            A PaginatedCollection of LabelerPerformance objects.
        """
        project_id_param = "projectId"
        query_str = """query LabelerPerformancePyApi($%s: ID!) {
            project(where: {id: $%s}) {
                labelerPerformance(skip: %%d first: %%d) {
                    count user {%s} secondsPerLabel totalTimeLabeling consensus
                    averageBenchmarkAgreement lastActivityTime}
            }}""" % (project_id_param, project_id_param,
                     query.results_query_part(Entity.named("User")))

        def create_labeler_performance(client, result):
            result["user"] = Entity.named("User")(client, result["user"])
            result["lastActivityTime"] = datetime.fromtimestamp(
                result["lastActivityTime"] / 1000, timezone.utc)
            return LabelerPerformance(**{utils.snake_case(key): value
                                         for key, value in result.items()})

        return PaginatedCollection(
            self.client, query_str, {project_id_param: self.uid},
            ["project", "labelerPerformance"], create_labeler_performance)

    def review_metrics(self, net_score):
        """ Returns this Project's review metrics.
        Args:
            net_score (None or Review.NetScore): Indicates desired metric.
        Return:
            int, aggregation count of reviews for given net_score.
        """
        if net_score not in (None,) + tuple(Entity.named("Review").NetScore):
            raise InvalidQueryError("Review metrics net score must be either None "
                                    "or one of Review.NetScore values")
        project_id_param = "project_id"
        net_score_literal = "None" if net_score is None else net_score.name
        query_str = """query ProjectReviewMetricsPyApi($%s: ID!){
            project(where: {id:$%s})
            {reviewMetrics {labelAggregate(netScore: %s) {count}}}
        }""" % (project_id_param, project_id_param, net_score_literal)
        res = self.client.execute(query_str, {project_id_param: self.uid})
        return res["data"]["project"]["reviewMetrics"]["labelAggregate"]["count"]

    def setup(self, labeling_frontend, labeling_frontend_options):
        """ Finalizes the Project setup.
        Args:
            labeling_frontend (LabelingFrontend): The labeling frontend to use.
            labeling_frontend_options (dict or str): Labeling frontend options,
                a.k.a. project ontology. If given a `dict` it will be converted
                to `str` using `json.dumps`.
        """
        organization = self.client.get_organization()
        if not isinstance(labeling_frontend_options, str):
            labeling_frontend_options = json.dumps(labeling_frontend_options)

        LFO = Entity.named("LabelingFrontendOptions")
        labeling_frontend_options = self.client._create(
            LFO, {LFO.project: self, LFO.labeling_frontend: labeling_frontend,
                  LFO.customization_options: labeling_frontend_options,
                  LFO.organization: organization
                  })

        self.labeling_frontend.connect(labeling_frontend)
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.update(setup_complete=timestamp)

    def set_labeling_parameter_overrides(self, data):
        """ Adds labeling parameter overrides to this project.
        Args:
            data (iterable): An iterable of tuples. Each tuple must contain
                (DataRow, priority, numberOfLabels) for the new override.
        Return:
            bool indicating if the operation was a success.
        """
        data_str = ",\n".join(
            "{dataRow: {id: \"%s\"}, priority: %d, numLabels: %d }" % (
                data_row.uid, priority, num_labels)
            for data_row, priority, num_labels in data)
        project_param = "projectId"
        query_str = """mutation setLabelingParameterOverridesPyApi($%s: ID!){
            project(where: { id: $%s }) {setLabelingParameterOverrides
            (data: [%s]) {success}}} """ % (project_param, project_param, data_str)
        res = self.client.execute(query_str, {project_param: self.uid})
        return res["data"]["project"]["setLabelingParameterOverrides"]["success"]

    def unset_labeling_parameter_overrides(self, data_rows):
        """ Removes labeling parameter overrides to this project.
        Args:
            data_rows (iterable): An iterable of DataRows.
        Return:
            bool indicating if the operation was a success.
        """
        project_param = "projectId"
        query_str = """mutation unsetLabelingParameterOverridesPyApi($%s: ID!){
            project(where: { id: $%s}) {
            unsetLabelingParameterOverrides(data: [%s]) { success }}}""" % (
            project_param, project_param,
            ",\n".join("{dataRowId: \"%s\"}" % row.uid for row in data_rows))
        res = self.client.execute(query_str, {project_param: self.uid})
        return res["data"]["project"]["unsetLabelingParameterOverrides"]["success"]

    def upsert_review_queue(self, quota_factor):
        """ Reinitiate the review queue for this project.
        Args:
            quota_factor (float): Which part (percentage) of the queue
                to reinitiate. Between 0 and 1.
        """
        project_param = "projectId"
        quota_param = "quotaFactor"
        query_str = """mutation UpsertReviewQueuePyApi($%s: ID!, $%s: Float!){
            upsertReviewQueue(where:{project: {id: $%s}}
                            data:{quotaFactor: $%s}) {id}}""" % (
            project_param, quota_param, project_param, quota_param)
        res = self.client.execute(
            query_str, {project_param: self.uid, quota_param: quota_factor})


    def extend_reservations(self, queue_type):
        """ Extend all the current reservations for the current user on the given
        queue type.
        Args:
            queue_type (str): Either "LabelingQueue" or "ReviewQueue"
        Return:
            int, the number of reservations that were extended.
        """
        if queue_type not in ("LabelingQueue", "ReviewQueue"):
            raise InvalidQueryError("Unsupported queue type: %s" % queue_type)

        project_param = "projectId"
        query_str = """mutation ExtendReservationsPyApi($%s: ID!){
            extendReservations(projectId:$%s queueType:%s)}""" % (
                project_param, project_param, queue_type)
        res = self.client.execute(query_str, {project_param: self.uid})
        return res["data"]["extendReservations"]

    def create_prediction_model(self, name, version):
        """ Creates a PredictionModel connected to this Project.
        Args:
            name (str): The new PredictionModel's name.
            version (int): The new PredictionModel's version.
        Return:
            A newly created PredictionModel.
        """
        PM = Entity.named("PredictionModel")
        model =  self.client._create(
            PM, {PM.name.name: name, PM.version.name: version})
        self.active_prediction_model.connect(model)
        return model

    def create_prediction(self, label, data_row, prediction_model=None):
        """ Creates a Prediction within this Project.
        Args:
            label (str): The `label` field of the new Prediction.
            data_row (DataRow): The DataRow for which the Prediction is created.
            prediction_model (PredictionModel or None): The PredictionModel
                within which the new Prediction is created. If None then this
                Project's active_prediction_model is used.
        Return:
            A newly created Prediction.
        Raises:
            labelbox.excepions.InvalidQueryError: if given `prediction_model`
                is None and this Project's active_prediction_model is also
                None.
        """
        if prediction_model is None:
            prediction_model = self.active_prediction_model()
            if prediction_model is None:
                raise InvalidQueryError(
                    "Project '%s' has no active prediction model" % self.name)

        label_param = "label"
        model_param = "prediction_model_id"
        project_param = "project_id"
        data_row_param = "data_row_id"

        Prediction = Entity.named("Prediction")
        query_str = """mutation CreatePredictionPyApi(
            $%s: String!, $%s: ID!, $%s: ID!, $%s: ID!) {createPrediction(
            data: {label: $%s, predictionModelId: $%s, projectId: $%s,
                   dataRowId: $%s})
            {%s}}""" % (label_param, model_param, project_param, data_row_param,
                        label_param, model_param, project_param, data_row_param,
                        query.results_query_part(Prediction))
        params = {label_param: label, model_param: prediction_model.uid,
                  data_row_param: data_row.uid, project_param: self.uid}
        res = self.client.execute(query_str, params)
        return Prediction(self.client, res["data"]["createPrediction"])


class LabelingParameterOverride(DbObject):
    priority = Field.Int("priority")
    number_of_labels = Field.Int("number_of_labels")


LabelerPerformance = namedtuple(
    "LabelerPerformance", "user count seconds_per_label, total_time_labeling "
    "consensus average_benchmark_agreement last_activity_time")
LabelerPerformance.__doc__ = "Named tuple containing info about a labeler's " \
    "performance."
