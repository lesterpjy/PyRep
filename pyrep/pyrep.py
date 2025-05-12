import numpy as np
from contextlib import contextmanager
from pyrep.backend import sim, utils
from pyrep.const import Verbosity, ObjectType
from pyrep.objects.object import Object
from pyrep.objects.shape import Shape
from pyrep.textures.texture import Texture
from pyrep.errors import PyRepError
from pyrep.backend import sim
from pyrep.backend.simConst import (
    sim_stringparam_verbosity,
    sim_stringparam_scene_path_and_name,
    sim_verbosity_traceall,
)

import os
import sys
import time
import threading
from typing import Tuple, List
import warnings
import traceback


class PyRep(object):
    """Used for interfacing with the CoppeliaSim simulation.

    Can be used for starting, stopping, and stepping the simulation. As well
    as getting, and creating scene objects and robots.
    """

    def __init__(self):
        self.running = False
        self._process = None
        self._robot_to_count = {}
        self.connected = False

        self._ui_thread = None
        self._responsive_ui_thread = None

        self._init_thread_id = None
        self._shutting_down = False

        self._handles_to_objects = {}

        if "COPPELIASIM_ROOT" not in os.environ:
            raise PyRepError(
                "COPPELIASIM_ROOT not defined. See installation instructions."
            )
        self._vrep_root = os.environ["COPPELIASIM_ROOT"]
        if not os.path.exists(self._vrep_root):
            raise PyRepError(
                "COPPELIASIM_ROOT was not a correct path. "
                "See installation instructions"
            )

    # def _run_ui_thread(
    #     self, scene_file: str, headless: bool, verbosity: Verbosity
    # ) -> None:
    #     # Need this otherwise extensions will not be loaded
    #     os.chdir(self._vrep_root)
    #     options = sim.sim_gui_headless if headless else sim.sim_gui_all
    #     sim.simSetStringParameter(sim.sim_stringparam_verbosity, verbosity.value)
    #     sim.simExtLaunchUIThread(
    #         options=options, scene=scene_file, pyrep_root=self._vrep_root
    #     )

    def _run_ui_thread(
        self, scene_file: str, headless: bool, verbosity: Verbosity
    ) -> None:
        os.chdir(self._vrep_root)
        options = sim.sim_gui_headless if headless else sim.sim_gui_all
        # --- FORCE MAXIMUM VERBOSITY FOR DEBUGGING SCENE LOAD ---
        print(
            f"PYREP_LAUNCH_DEBUG: Forcing CoppeliaSim verbosity to TRACEALL ({sim_verbosity_traceall})"
        )
        sim.lib.simSetStringParameter(
            sim_stringparam_verbosity, str(sim_verbosity_traceall).encode("ascii")
        )  # Force highest
        # --- END FORCE VERBOSITY ---
        sim.lib.simExtLaunchUIThread(  # Use sim.lib
            "PyRep".encode("ascii"),
            options,
            scene_file.encode("ascii"),
            self._vrep_root.encode("ascii"),
        )

    def _run_responsive_ui_thread(self) -> None:
        while True:
            if not self.running:
                with utils.step_lock:
                    if self._shutting_down or sim.simExtGetExitRequest():
                        break
                    sim.simExtStep(False)
            time.sleep(0.01)
        # If the exit request was from the UI, then call shutdown, otherwise
        # shutdown caused this thread to terminate.
        if not self._shutting_down:
            self.shutdown()

    def launch(
        self,
        scene_file: str = "",
        headless: bool = False,
        responsive_ui: bool = False,
        blocking: bool = False,
        verbosity: Verbosity = Verbosity.NONE,
    ) -> None:
        abs_scene_file = os.path.abspath(scene_file)
        if len(scene_file) > 0 and not os.path.isfile(abs_scene_file):
            print(
                f"PYREP_LAUNCH_DEBUG: ERROR - Scene file specified BUT DOES NOT EXIST: {abs_scene_file}"
            )  # More explicit
            raise PyRepError("Scene file does not exist: %s" % scene_file)
        elif len(scene_file) == 0:
            print(
                f"PYREP_LAUNCH_DEBUG: No scene file specified, launching with empty scene."
            )
            abs_scene_file = ""  # Ensure empty string if none provided
        else:
            print(
                f"PYREP_LAUNCH_DEBUG: Scene file specified and found: {abs_scene_file}"
            )

        print(
            f"PYREP_LAUNCH_DEBUG: Calling simExtLaunchUIThread with scene: '{abs_scene_file}'"
        )
        cwd = os.getcwd()
        self._ui_thread = threading.Thread(
            target=self._run_ui_thread, args=(abs_scene_file, headless, verbosity)
        )
        self._ui_thread.daemon = True
        self._ui_thread.start()

        while not sim.lib.simExtCanInitSimThread():  # Use sim.lib
            time.sleep(0.1)

        sim.lib.simExtSimThreadInit()  # Use sim.lib
        time.sleep(0.2)

        # === NEW: Perform a preliminary step to ensure scene is "active" ===
        print(f"PYREP_LAUNCH_DEBUG: Before preliminary step().")
        try:
            with utils.step_lock:  # Use the same lock as self.step()
                sim.lib.simExtStep(
                    True
                )  # True to advance simulation if running (it's not yet)
            print(f"PYREP_LAUNCH_DEBUG: Preliminary step() completed.")
            time.sleep(0.1)  # Another small pause
        except Exception as e_prelim_step:
            print(f"PYREP_LAUNCH_DEBUG: ERROR during preliminary step: {e_prelim_step}")
        # === END NEW ===

        print(
            f"PYREP_LAUNCH_DEBUG: simExtSimThreadInit completed (and preliminary step done). Scene '{abs_scene_file}' should be loaded."
        )

        print(
            f"PYREP_LAUNCH_DEBUG: simExtSimThreadInit completed. Scene '{abs_scene_file}' should be loaded."
        )
        print(f"PYREP_LAUNCH_DEBUG: Attempting to list objects in the scene NOW...")
        # Try to get the scene name that CoppeliaSim *thinks* is loaded
        try:
            loaded_scene_name_ptr = sim.lib.simGetStringParameter(
                sim_stringparam_scene_path_and_name
            )
            if loaded_scene_name_ptr != sim.ffi.NULL:
                loaded_scene_name = sim.ffi.string(loaded_scene_name_ptr).decode(
                    "utf-8"
                )
                print(
                    f"PYREP_LAUNCH_DEBUG: CoppeliaSim reports current scene as: '{loaded_scene_name}'"
                )
                sim.lib.simReleaseBuffer(loaded_scene_name_ptr)
                if abs_scene_file and loaded_scene_name != abs_scene_file:
                    print(
                        f"PYREP_LAUNCH_DEBUG: WARNING! Scene mismatch. Requested: '{abs_scene_file}', CoppeliaSim loaded: '{loaded_scene_name}'"
                    )
            else:
                print(
                    f"PYREP_LAUNCH_DEBUG: CoppeliaSim reports NO current scene name (simGetStringParameter returned NULL)."
                )
        except Exception as e_scene_name_early:
            print(
                f"PYREP_LAUNCH_DEBUG: Error getting scene name immediately after simExtSimThreadInit: {e_scene_name_early}"
            )

        print(
            f"PYREP_LAUNCH_DEBUG: Attempting to list objects in the scene NOW (immediately after simExtSimThreadInit)..."
        )

        try:
            object_count_ptr = sim.ffi.new("int*")
            # Call the C API function directly via sim.lib
            handles_ptr = sim.lib.simGetObjectsInTree(
                sim.sim_handle_scene, ObjectType.ALL.value, 0, object_count_ptr
            )

            object_names = []
            if handles_ptr != sim.ffi.NULL and object_count_ptr[0] > 0:
                num_objects = object_count_ptr[0]
                print(f"PYREP_LAUNCH_DEBUG: Found {num_objects} object handles.")
                for i in range(num_objects):
                    handle_val = handles_ptr[i]
                    name_ptr = sim.lib.simGetObjectName(handle_val)
                    if name_ptr != sim.ffi.NULL:
                        try:
                            obj_name_str = sim.ffi.string(name_ptr).decode("utf-8")
                            object_names.append(
                                f"{obj_name_str} (handle: {handle_val})"
                            )
                        finally:
                            sim.lib.simReleaseBuffer(name_ptr)
                sim.lib.simReleaseBuffer(sim.ffi.cast("char *", handles_ptr))
            else:
                print(
                    f"PYREP_LAUNCH_DEBUG: simGetObjectsInTree returned no objects or an error (count: {object_count_ptr[0]}). Scene might be empty or not loaded."
                )

            print(
                f"PYREP_LAUNCH_DEBUG: Objects in scene immediately after launch: {sorted(list(set(object_names)))}"
            )
        except Exception as list_e:
            print(
                f"PYREP_LAUNCH_DEBUG: Error encountered while trying to list objects after launch: {type(list_e).__name__} - {list_e}"
            )
            traceback.print_exc()

        if blocking:
            while not sim.lib.simExtGetExitRequest():  # Use sim.lib
                sim.lib.simExtStep(True)  # Use sim.lib, pass True or False
            self.shutdown()
        elif responsive_ui:
            # ... (responsive_ui logic, ensure sim.lib calls if needed) ...
            self._responsive_ui_thread = threading.Thread(
                target=self._run_responsive_ui_thread
            )
            self._responsive_ui_thread.daemon = True
            try:
                self._responsive_ui_thread.start()
            except (KeyboardInterrupt, SystemExit):
                if not self._shutting_down:
                    self.shutdown()
                sys.exit()
            self.step()  # Initial step
        else:
            self.step()  # Initial step

        if not blocking:
            try:
                # Use the correct constant sim_stringparam_scene_path_and_name
                current_scene_name_ptr = sim.lib.simGetStringParameter(
                    sim_stringparam_scene_path_and_name
                )
                if current_scene_name_ptr != sim.ffi.NULL:
                    current_scene_name = sim.ffi.string(current_scene_name_ptr).decode(
                        "utf-8"
                    )
                    sim.lib.simReleaseBuffer(current_scene_name_ptr)
                    print(
                        f"PYREP_LAUNCH_DEBUG: After first main logic step(s). Current scene: {current_scene_name}"
                    )
                else:
                    print(
                        f"PYREP_LAUNCH_DEBUG: After first main logic step(s). Current scene: (simGetStringParameter returned NULL for scene_path_and_name)"
                    )
            except Exception as e_scene_name:
                print(
                    f"PYREP_LAUNCH_DEBUG: After first main logic step(s). Error getting scene name: {e_scene_name}"
                )
        os.chdir(cwd)

    def script_call(
        self,
        function_name_at_script_name: str,
        script_handle_or_type: int,
        ints=(),
        floats=(),
        strings=(),
        bytes="",
    ) -> Tuple[List[int], List[float], List[str], str]:
        """Calls a script function (from a plugin, the main client application,
        or from another script). This represents a callback inside of a script.

        :param function_name_at_script_name: A string representing the function
            name and script name, e.g. myFunctionName@theScriptName. When the
            script is not associated with an object, then just specify the
            function name.
        :param script_handle_or_type: The handle of the script, otherwise the
            type of the script.
        :param ints: The input ints to the script.
        :param floats: The input floats to the script.
        :param strings: The input strings to the script.
        :param bytes: The input bytes to the script (as a string).
        :return: Any number of return values from the called Lua function.
        """
        return utils.script_call(
            function_name_at_script_name,
            script_handle_or_type,
            ints,
            floats,
            strings,
            bytes,
        )

    def shutdown(self) -> None:
        """Shuts down the CoppeliaSim simulation."""
        if self._ui_thread is None:
            raise PyRepError("CoppeliaSim has not been launched. Call launch first.")
        if self._ui_thread is not None:
            self._shutting_down = True
            self.stop()
            self.step_ui()
            sim.simExtPostExitRequest()
            sim.simExtSimThreadDestroy()
            self._ui_thread.join()
            if self._responsive_ui_thread is not None:
                self._responsive_ui_thread.join()
            # CoppeliaSim crashes if new instance opened too quickly after shutdown.
            # TODO: A small sleep stops this for now.
            time.sleep(0.1)
        self._ui_thread = None
        self._shutting_down = False

    def start(self) -> None:
        """Starts the physics simulation if it is not already running."""
        if self._ui_thread is None:
            raise PyRepError("CoppeliaSim has not been launched. Call launch first.")
        if not self.running:
            sim.simStartSimulation()
            self.running = True

    def stop(self) -> None:
        """Stops the physics simulation if it is running."""
        if self._ui_thread is None:
            raise PyRepError("CoppeliaSim has not been launched. Call launch first.")
        if self.running:
            sim.simStopSimulation()
            self.running = False
            # Need this so the UI updates
            [self.step() for _ in range(5)]  # type: ignore

    def step(self) -> None:
        """Execute the next simulation step.

        If the physics simulation is not running, then this will only update
        the UI.
        """
        with utils.step_lock:
            sim.simExtStep()

    def step_ui(self) -> None:
        """Update the UI.

        This will not execute the next simulation step, even if the physics
        simulation is running.
        This is only applicable when PyRep was launched without a responsive UI.
        """
        with utils.step_lock:
            sim.simExtStep(False)

    def set_simulation_timestep(self, dt: float) -> None:
        """Sets the simulation time step. Default is 0.05.

        :param dt: The time step value in seconds.
        """
        sim.simSetFloatParameter(sim.sim_floatparam_simulation_time_step, dt)
        if not np.allclose(self.get_simulation_timestep(), dt):
            warnings.warn(
                "Could not change simulation timestep. You may need "
                'to change it to "custom dt" using simulation '
                "settings dialog."
            )

    def get_simulation_timestep(self) -> float:
        """Gets the simulation time step.

        :return: The time step value in seconds.
        """
        return sim.simGetSimulationTimeStep()

    def set_configuration_tree(self, config_tree: bytes) -> None:
        """Restores configuration information previously retrieved.

        Configuration information (object relative positions/orientations,
        joint/path values) can be retrieved with
        :py:meth:`Object.get_configuration_tree`. Dynamically simulated
        objects will implicitly be reset before the command is applied
        (i.e. similar to calling :py:meth:`Object.reset_dynamic_object` just
        before).

        :param config_tree: The configuration tree to restore.
        """
        sim.simSetConfigurationTree(config_tree)

    def group_objects(self, objects: List[Shape]) -> Shape:
        """Groups several shapes into a compound shape (or simple shape).

        :param objects: The list of shapes to group.
        :return: A single grouped shape.
        """
        handles = [o.get_handle() for o in objects]
        handle = sim.simGroupShapes(handles)
        return Shape(handle)

    def merge_objects(self, objects: List[Shape]) -> Shape:
        """Merges several shapes into a compound shape (or simple shape).

        :param objects: The list of shapes to group.
        :return: A single merged shape.
        """
        handles = [o.get_handle() for o in objects]
        # FIXME: sim.simGroupShapes(merge=True) won't return correct handle,
        # so we use name to find correct handle of the merged shape.
        name = objects[-1].get_name()
        sim.simGroupShapes(handles, merge=True)
        return Shape(name)

    def export_scene(self, filename: str) -> None:
        """Saves the current scene.

        :param filename: scene filename. The filename extension is required
            ("ttt").
        """
        sim.simSaveScene(filename)

    def import_model(self, filename: str) -> Object:
        """Loads a previously saved model.

        :param filename: model filename. The filename extension is required
            ("ttm"). An optional "@copy" can be appended to the filename, in
            which case the model's objects will be named/renamed as if an
            associated script was attached to the model.
        :return: The imported model.
        """
        handle = sim.simLoadModel(filename)
        return utils.to_type(handle)

    def create_texture(
        self,
        filename: str,
        interpolate=True,
        decal_mode=False,
        repeat_along_u=False,
        repeat_along_v=False,
    ) -> Tuple[Shape, Texture]:
        """Creates a planar shape that is textured.

        :param filename: Path to the texture to load.
        :param interpolate: Adjacent texture pixels are not interpolated.
        :param decal_mode: Texture is applied as a decal (its appearance
            won't be influenced by light conditions).
        :param repeat_along_u: Texture will be repeated along the U direction.
        :param repeat_along_v: Texture will be repeated along the V direction.
        :return: A tuple containing the textured plane and the texture.
        """
        options = 0
        if not interpolate:
            options |= 1
        if decal_mode:
            options |= 2
        if repeat_along_u:
            options |= 3
        if repeat_along_v:
            options |= 4
        handle = sim.simCreateTexture(filename, options)
        s = Shape(handle)
        return s, s.get_texture()

    def get_objects_in_tree(self, root_object=None, *args, **kwargs) -> List[Object]:
        """Retrieves the objects in a given hierarchy tree.

        :param root_object: The root object in the tree. Pass None to retrieve
            all objects in the configuration tree. :py:class:`Object` or `int`.
        :param object_type: The object type to retrieve.
            One of :py:class:`.ObjectType`.
        :param exclude_base: Exclude the tree base from the returned list.
        :param first_generation_only: Include in the returned list only the
            object's first children. Otherwise, entire hierarchy is returned.
        :return: A list of objects in the hierarchy tree.
        """
        return Object._get_objects_in_tree(root_object, *args, **kwargs)

    def get_collection_handle_by_name(self, collection_name: str) -> int:
        """Retrieves the integer handle for a given collection.

        :param collection_name: Name of the collection to retrieve the integer handle for
        :return: An integer handle for the collection
        """
        return sim.simGetCollectionHandle(collection_name)
