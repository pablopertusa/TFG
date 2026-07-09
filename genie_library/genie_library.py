from databricks.sdk.service.dashboards import GenieSpace, GenieConversationSummary, GenieMessage, GenieFeedbackRating
from databricks.sdk.service.tags import TagAssignment
from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel
from collections import Counter
import datetime
from genie_library.base_classes import LoggingClass, DatabricksController
from genie_library.enums import Environment
import json
import yaml
from typing import Self, Callable
from pathlib import Path
from datetime import date, timedelta
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
                                                    
class GenieSpaceController(LoggingClass, DatabricksController):
    """
    Class used to perform actions in Genie Spaces
    """
    def __init__(self, space: GenieSpace, environment: Environment = Environment.databricks_runtime):
        self.space = space
        self.space_id = space.space_id
        self.logger = super().init_logger()
        self.w = super().get_workspace_client(environment=environment)
    
    @classmethod
    def from_id(cls, space_id: str, include_serialized_space: bool = True, environment: Environment = Environment.databricks_runtime) -> Self:
        w = cls.get_workspace_client(environment=environment)
        space = w.genie.get_space(space_id=space_id, include_serialized_space=include_serialized_space)
        return cls(space)
    
    @classmethod
    def list_genie_spaces(cls, environment: Environment = Environment.databricks_runtime) -> list[GenieSpace] | None:
        w = cls.get_workspace_client(environment=environment)
        return w.genie.list_spaces().spaces
    
    def save_space(self):
        self.w.genie.update_space(**self.space.as_dict())

    def list_conversations(self, include_all: bool = True) -> list[GenieConversationSummary] | None:
        conversations_list: list[GenieConversationSummary] = []
        conversations_response = self.w.genie.list_conversations(space_id=self.space_id, include_all=include_all, page_size=50)
        if conversations_response.conversations:
            conversations_list.extend(conversations_response.conversations)
            while conversations_response.next_page_token != None:
                conversations_response = self.w.genie.list_conversations(space_id=self.space_id, include_all=include_all, page_size=5, page_token=conversations_response.next_page_token)
                if conversations_response.conversations:
                    conversations_list.extend(conversations_response.conversations)
        return conversations_list
    
    def list_conversation_messages(self, conversation_id: str) -> list[GenieMessage] | None:
        messages_list: list[GenieMessage] = []
        messages_response = self.w.genie.list_conversation_messages(space_id=self.space_id, conversation_id=conversation_id, page_size=20)
        if messages_response.messages:
            messages_list.extend(messages_response.messages)
            while messages_response.next_page_token != None:
                messages_response = self.w.genie.list_conversation_messages(space_id=self.space_id, conversation_id=conversation_id, page_size=20, page_token=messages_response.next_page_token)
                if messages_response.messages:
                    messages_list.extend(messages_response.messages)
        return messages_list

    def list_messages(self, include_all: bool = True) -> list[GenieMessage]:
        result = []
        conversations = self.list_conversations(include_all=include_all)
        if not conversations:
            return result
        for conversation in conversations:
            messages = self.list_conversation_messages(conversation_id=conversation.conversation_id)
            if messages:
                result.extend(messages)
        return result

    @staticmethod
    def _datetime_from_timestamp(timestamp_ms: int | None) -> datetime.datetime | None:
        if timestamp_ms is None:
            return None
        return datetime.datetime.fromtimestamp(timestamp_ms / 1000, tz=datetime.timezone.utc)

    @staticmethod
    def _rating_value(rating: GenieFeedbackRating | str | None) -> str | None:
        if rating is None:
            return None
        if isinstance(rating, GenieFeedbackRating):
            return rating.value
        return rating

    @classmethod
    def _thumb_key(cls, message: GenieMessage) -> str | None:
        if not message.feedback:
            return None
        rating = cls._rating_value(message.feedback.rating)
        if rating == GenieFeedbackRating.POSITIVE.value:
            return "thumbs_up"
        if rating == GenieFeedbackRating.NEGATIVE.value:
            return "thumbs_down"
        return None

    @staticmethod
    def _sorted_counter(counter: Counter) -> dict:
        return dict(sorted(counter.items(), key=lambda item: str(item[0])))

    @staticmethod
    def _period_start_from_key(grain: str, period_key: str | None) -> date | None:
        if not period_key:
            return None
        if grain == "day":
            return date.fromisoformat(period_key)
        if grain == "week":
            year, week = period_key.split("-W")
            return date.fromisocalendar(int(year), int(week), 1)
        if grain == "month":
            return date.fromisoformat(f"{period_key}-01")
        return None

    def _history_metric_row(
        self,
        snapshot_ts: datetime.datetime,
        metric_name: str,
        grain: str,
        value: int,
        period_key: str | None = None,
        dimension_name: str | None = None,
        dimension_value: str | None = None,
    ) -> dict:
        return {
            "snapshot_ts": snapshot_ts,
            "space_id": self.space_id,
            "space_title": self.space.title,
            "metric_name": metric_name,
            "grain": grain,
            "period_start": self._period_start_from_key(grain=grain, period_key=period_key),
            "period_key": period_key,
            "dimension_name": dimension_name,
            "dimension_value": dimension_value,
            "value": int(value),
        }

    def get_questions_per_user(self) -> dict[str, int]:
        counter = Counter()
        user_names = self.get_user_names()
        for message in self.list_messages():
            user_name = self.get_user_name(user_id=message.user_id, user_names=user_names)
            counter[user_name] += 1
        return self._sorted_counter(counter)

    def get_questions_per_day(self) -> dict[str, int]:
        counter = Counter()
        for message in self.list_messages():
            message_datetime = self._datetime_from_timestamp(message.created_timestamp)
            if message_datetime:
                counter[message_datetime.date().isoformat()] += 1
        return self._sorted_counter(counter)

    def get_questions_per_week(self) -> dict[str, int]:
        counter = Counter()
        for message in self.list_messages():
            message_datetime = self._datetime_from_timestamp(message.created_timestamp)
            if message_datetime:
                iso_year, iso_week, _ = message_datetime.isocalendar()
                counter[f"{iso_year}-W{iso_week:02d}"] += 1
        return self._sorted_counter(counter)

    def get_questions_per_month(self) -> dict[str, int]:
        counter = Counter()
        for message in self.list_messages():
            message_datetime = self._datetime_from_timestamp(message.created_timestamp)
            if message_datetime:
                counter[message_datetime.strftime("%Y-%m")] += 1
        return self._sorted_counter(counter)

    def get_total_questions_history(self) -> int:
        return len(self.list_messages())

    def get_total_thumbs_history(self) -> dict[str, int]:
        result = {"thumbs_up": 0, "thumbs_down": 0}
        for message in self.list_messages():
            thumb_key = self._thumb_key(message)
            if thumb_key:
                result[thumb_key] += 1
        return result

    def get_thumbs_per_month(self) -> dict[str, dict[str, int]]:
        result = {}
        for message in self.list_messages():
            thumb_key = self._thumb_key(message)
            message_datetime = self._datetime_from_timestamp(message.created_timestamp)
            if thumb_key and message_datetime:
                month_key = message_datetime.strftime("%Y-%m")
                if month_key not in result:
                    result[month_key] = {"thumbs_up": 0, "thumbs_down": 0}
                result[month_key][thumb_key] += 1
        return dict(sorted(result.items()))
    
    @classmethod
    def get_user_names(cls, environment: Environment = Environment.databricks_runtime) -> dict[str, str]:
        w = cls.get_workspace_client(environment=environment)
        all_users = w.users.list(attributes="id,userName")
        return {str(user.id): user.user_name for user in all_users if user.id and user.user_name}
    
    @staticmethod
    def get_user_name(user_id: int | str | None, user_names: dict[str, str]) -> str:
        if user_id is None:
            return "unknown"
        return user_names.get(str(user_id), "unknown")

    def get_history_metrics(self) -> dict:
        messages = self.list_messages()
        questions_per_user = Counter()
        questions_per_day = Counter()
        questions_per_week = Counter()
        questions_per_month = Counter()
        total_thumbs = {"thumbs_up": 0, "thumbs_down": 0}
        thumbs_per_month = {}
        user_names = self.get_user_names()

        for message in messages:
            user_name = self.get_user_name(user_id=message.user_id, user_names=user_names)
            questions_per_user[user_name] += 1

            message_datetime = self._datetime_from_timestamp(message.created_timestamp)
            month_key = None
            if message_datetime:
                questions_per_day[message_datetime.date().isoformat()] += 1
                iso_year, iso_week, _ = message_datetime.isocalendar()
                questions_per_week[f"{iso_year}-W{iso_week:02d}"] += 1
                month_key = message_datetime.strftime("%Y-%m")
                questions_per_month[month_key] += 1

            thumb_key = self._thumb_key(message)
            if thumb_key:
                total_thumbs[thumb_key] += 1
                if message_datetime:
                    if month_key and month_key not in thumbs_per_month:
                        thumbs_per_month[month_key] = {"thumbs_up": 0, "thumbs_down": 0}
                    thumbs_per_month[month_key][thumb_key] += 1

        return {
            "questions_per_user": self._sorted_counter(questions_per_user),
            "questions_per_day": self._sorted_counter(questions_per_day),
            "questions_per_week": self._sorted_counter(questions_per_week),
            "questions_per_month": self._sorted_counter(questions_per_month),
            "total_questions_history": len(messages),
            "total_thumbs_history": total_thumbs,
            "thumbs_per_month": dict(sorted(thumbs_per_month.items())),
        }

    def flatten_history_metrics(
        self,
        metrics: dict | None = None,
        snapshot_ts: datetime.datetime | None = None,
    ) -> list[dict]:
        if metrics is None:
            metrics = self.get_history_metrics()
        if snapshot_ts is None:
            snapshot_ts = datetime.datetime.now(tz=datetime.timezone.utc)

        rows = []

        for user_name, value in metrics.get("questions_per_user", {}).items():
            rows.append(self._history_metric_row(
                snapshot_ts=snapshot_ts,
                metric_name="questions",
                grain="user",
                value=value,
                dimension_name="user_name",
                dimension_value=str(user_name),
            ))

        for period_key, value in metrics.get("questions_per_day", {}).items():
            rows.append(self._history_metric_row(
                snapshot_ts=snapshot_ts,
                metric_name="questions",
                grain="day",
                period_key=period_key,
                value=value,
            ))

        for period_key, value in metrics.get("questions_per_week", {}).items():
            rows.append(self._history_metric_row(
                snapshot_ts=snapshot_ts,
                metric_name="questions",
                grain="week",
                period_key=period_key,
                value=value,
            ))

        for period_key, value in metrics.get("questions_per_month", {}).items():
            rows.append(self._history_metric_row(
                snapshot_ts=snapshot_ts,
                metric_name="questions",
                grain="month",
                period_key=period_key,
                value=value,
            ))

        rows.append(self._history_metric_row(
            snapshot_ts=snapshot_ts,
            metric_name="questions",
            grain="history",
            value=metrics.get("total_questions_history", 0),
        ))

        for metric_name, value in metrics.get("total_thumbs_history", {}).items():
            rows.append(self._history_metric_row(
                snapshot_ts=snapshot_ts,
                metric_name=metric_name,
                grain="history",
                value=value,
            ))

        for period_key, thumbs in metrics.get("thumbs_per_month", {}).items():
            for metric_name, value in thumbs.items():
                rows.append(self._history_metric_row(
                    snapshot_ts=snapshot_ts,
                    metric_name=metric_name,
                    grain="month",
                    period_key=period_key,
                    value=value,
                ))

        return rows
      
    def get_positive_messages_from_conversation(self, conversation_id: str, timestamp: datetime.datetime | None = None) -> list[GenieMessage] | None:
        result = []
        messages = self.list_conversation_messages(conversation_id=conversation_id)
        if not messages:
            return
        for message in messages:
            if message.feedback and message.feedback.rating == GenieFeedbackRating.POSITIVE:
                if not timestamp or timestamp and message.created_timestamp and message.created_timestamp > int(timestamp.timestamp() * 1000):
                    result.append(message)
                else:
                    self.logger.debug(f"Skipped positive message (timestamp = {timestamp})")
        return result
    
    def get_positive_messages(self, timestamp: datetime.datetime | None = None) -> list[GenieMessage] | None:
        result = []
        conversations = self.list_conversations()
        if not conversations:
            return
        for conversation in conversations:
            positive_messages = self.get_positive_messages_from_conversation(conversation.conversation_id, timestamp = timestamp)
            if positive_messages:
                result.extend(positive_messages)
        return result
    
    def list_tags(self) -> list[TagAssignment]:
        return [t for t in self.w.workspace_entity_tag_assignments.list_tag_assignments(entity_id=self.space_id, entity_type="geniespaces")]
    
    def create_tag(self, tag_key: str, tag_value: str | None = None):
        tag = TagAssignment(entity_id=self.space_id, entity_type="geniespaces", tag_key=tag_key, tag_value=tag_value)
        self.w.workspace_entity_tag_assignments.create_tag_assignment(tag_assignment=tag)
    
    @classmethod
    def get_genie_spaces_by_tag(cls, tag_key: str, tag_value: str | None = None, include_serialized_space: bool = False,
                                enviroment: Environment = Environment.databricks_runtime) \
        -> list[GenieSpace]:
        result = []
        spaces = cls.list_genie_spaces(environment=enviroment)
        logger = cls.init_logger()
        if spaces:
            for space in spaces:
                controller = GenieSpaceController.from_id(space_id=space.space_id, include_serialized_space=include_serialized_space, environment=enviroment)
                tags = controller.list_tags()
                for t in tags:
                    if tag_value:
                        if t.tag_key == tag_key and t.tag_value == tag_value and space not in result:
                            result.append(space)
                    else:
                        if t.tag_key == tag_key and space not in result:
                                result.append(space)
        else:
            logger.debug("No spaces listed")
        return result

    @classmethod
    def get_history_metrics_by_tag(cls, tag_key: str, tag_value: str | None = None,
                                   environment: Environment = Environment.databricks_runtime) -> list[dict]:
        rows = []
        snapshot_ts = datetime.datetime.now(tz=datetime.timezone.utc)
        logger = cls.init_logger()
        spaces = cls.get_genie_spaces_by_tag(tag_key=tag_key, tag_value=tag_value, enviroment=environment)

        for space in spaces:
            try:
                controller = cls.from_id(space_id=space.space_id, include_serialized_space=False, environment=environment)
                rows.extend(controller.flatten_history_metrics(snapshot_ts=snapshot_ts))
            except Exception as exc:
                logger.error(f"Could not collect history metrics for Genie Space {space.space_id}: {exc}")

        return rows

    def run_benchmark(self) -> dict:
        return self.w.genie.genie_create_eval_run(self.space_id).as_dict()
    
    def get_benchmark_run(self, run_id: str) -> dict:
        return self.w.genie.genie_get_eval_run(space_id=self.space_id, eval_run_id=run_id).as_dict()

    def run_benchmark_and_wait_for_result(self) -> dict:
        run_id = self.run_benchmark()["eval_run_id"]
        run = self.get_benchmark_run(run_id=run_id)
        run_status = run["eval_run_status"]
        start_time = datetime.datetime.now()
        timeout = 20
        end_time_limit = start_time + timedelta(minutes=timeout)
        while run_status != "DONE":
            if datetime.datetime.now() > end_time_limit:
                raise TimeoutError(
                    f"Benchmark {run_id} was cancelled for exceeding {timeout} minutes"
                )
            self.logger.debug(f"Benchmark for {self.space.title} still running...")
            time.sleep(30)
            run = self.get_benchmark_run(run_id=run_id)
            run_status = run["eval_run_status"]
        result = {
            "num_correct": run["num_correct"],
            "num_done": run["num_done"],
            "space_id": self.space_id,
            "space_title": self.space.title
        }
        return result

    @classmethod
    def benchmark_by_tag(cls, tag_key: str, tag_value: str | None = None, callback: Callable | None = None,
                         environment: Environment = Environment.databricks_runtime):
        list_spaces = cls.get_genie_spaces_by_tag(tag_key=tag_key, tag_value=tag_value, enviroment=environment)
        list_controllers = [cls.from_id(space.space_id, include_serialized_space=True, environment=environment) for space in list_spaces]

        logger = cls.init_logger()
        logger.info(f"Starting benchmarks for {len(list_controllers)} Genie Spaces...")

        with ThreadPoolExecutor() as executor:
            future_to_controller = {
                executor.submit(controller.run_benchmark_and_wait_for_result): controller
                for controller in list_controllers
            }

            for future in as_completed(future_to_controller):
                controller = future_to_controller[future]
                try:
                    result = future.result()
                    logger.info(f"Benchmark ended for {controller.space.title}")

                    if callback:
                        try:
                            callback(result)
                        except Exception as e:
                            logger.error(f"Error calling the callback after the benchmark result: {e}")

                except Exception as exc:
                    logger.error(f"Genie Space {controller.space.title} ({controller.space_id}) threw an exception: {exc}")

    def serialize(self, output_file: str):
        space = self.w.genie.get_space(space_id=self.space_id, include_serialized_space=True)
        if space.serialized_space:
            serialized_space = json.loads(space.serialized_space)
            dict_space = space.as_dict()
            # Remove serialized for a cleaner file
            del dict_space["serialized_space"]
            complete_space = dict_space | {"serialized_space": serialized_space}
            try:
                with open(output_file, "w", encoding="utf-8") as file:
                    yaml.dump(complete_space, file, allow_unicode=True, default_flow_style=False)
                self.logger.debug(f"Created file {output_file}")

            except OSError:
                self.logger.error(f"Space with id = {space.space_id} during a serialize() call could not \
                                  open file {output_file}")
        else:
            self.logger.error(f"Space {space.title} with id = {space.space_id} has no serialized_space attribute, found\
                              in a serialized() call")
    
    @classmethod
    def restore(cls, input_file: str):
        logger = LoggingClass.init_logger()
        
        try:
            with open(input_file, "r", encoding="utf-8") as file:
                complete_space = yaml.safe_load(file)
                
            if not complete_space:
                logger.error(f"File {input_file} is empty or not a valid YAML.")
                return None

            if "serialized_space" in complete_space:
                serialized_data = complete_space["serialized_space"]
                complete_space["serialized_space"] = json.dumps(serialized_data)
            else:
                logger.warning(f"File {input_file} does not contain the 'serialized_space' attribute.")

            space = GenieSpace(**complete_space)
            
            new_controller = cls(space)
            new_controller.save_space()

            return new_controller

        except OSError:
            logger.error(f"Could not open file {input_file} during a restore() call.")
            return None
            
        except yaml.YAMLError as exc:
            logger.error(f"Error parsing YAML file {input_file}: {exc}")
            return None
    
    @classmethod
    def serialize_by_tag(cls, directory: str, tag_key: str, tag_value: str | None = None, environment: Environment = Environment.databricks_runtime):
        path = Path(directory)
        logger = cls.init_logger()
        if not path.exists():
            logger.debug(f"Directory {path} does not exist, creating it")
            path.mkdir(exist_ok=True)

        list_spaces = cls.get_genie_spaces_by_tag(tag_key=tag_key, tag_value=tag_value, enviroment=environment)
        list_controllers = [cls.from_id(space.space_id, include_serialized_space=True, environment=environment) for space in list_spaces]
        t = date.today().strftime("%Y-%m-%d")

        for controller in list_controllers:
            subdirectory_path = path / controller.space_id
            if not subdirectory_path.exists():
                logger.debug(f"Directory {subdirectory_path} does not exist, creating it")
                subdirectory_path.mkdir(exist_ok=True)
            controller.serialize(str(subdirectory_path / f"copy-{t}.yaml"))
    
    @staticmethod
    def get_permission_level(permission_level: str) -> PermissionLevel:
        if permission_level == "CAN_MANAGE":
            return PermissionLevel.CAN_MANAGE
        elif permission_level == "CAN_EDIT":
            return PermissionLevel.CAN_EDIT
        elif permission_level == "CAN_READ":
            return PermissionLevel.CAN_READ
        else:
            raise ValueError(f"{permission_level} is not a valid permission level, must be one of ['CAN_MANAGE', 'CAN_EDIT', 'CAN_READ']")

    @classmethod
    def assign_users_to_space(cls, space_id: str, user_list: list[str], permission_level: PermissionLevel = PermissionLevel.CAN_MANAGE, environment: Environment = Environment.databricks_runtime):
        permissions = [
            AccessControlRequest(user_name=user, permission_level=permission_level)
            for user in user_list
        ]
        w = cls.get_workspace_client(environment=environment)
        w.permissions.update(
            request_object_type="genie",
            request_object_id=space_id,
            access_control_list=permissions
        )

    @classmethod
    def list_space_permissions(cls, space_id: str, environment: Environment = Environment.databricks_runtime) -> list[dict]:
        w = cls.get_workspace_client(environment=environment)
        object_permissions = w.permissions.get(request_object_type="genie", request_object_id=space_id)
        access_control_list = object_permissions.access_control_list or []
        return [permission.as_dict() for permission in access_control_list]
