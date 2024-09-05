import itertools

from procset import ProcSet

from oar.lib.globals import get_logger, init_oar
from oar.lib.hierarchy import find_resource_hierarchies_scattered
from sqlalchemy import func, case, desc, asc
from sqlalchemy.orm import Session
from oar.lib.models import (
    AssignedResource,
    EventLog,
    FragJob,
    Job,
    JobType,
    MoldableJobDescription,
    Resource,
    ResourceLog
)
from oar.lib.resource import ResourceSet

logger = get_logger("oar.custom_scheduling")


def get_nodes_characterization(session: Session):
    # Subquery to calculate the appearance count and assign a row number per network_address
    subquery = (
        session.query(
            Resource.network_address,
            JobType.type,
            func.count(AssignedResource.resource_id).label('appearance_count'),
            func.row_number().over(
                partition_by=Resource.network_address,
                order_by=[
                    desc(func.count(AssignedResource.resource_id)),
                    case(
                        (JobType.type == 'unfriendly', 1),
                        (JobType.type == 'friendly', 2),
                        else_=3
                    )
                ]
            ).label('rn')
        )
        .join(Job, Job.id == JobType.job_id)
        .join(AssignedResource, Job.assigned_moldable_job == AssignedResource.moldable_id)
        .join(Resource, AssignedResource.resource_id == Resource.id)
        .filter(Job.state.in_(("toLaunch", "Running", "Resuming"))) # .filter(Job.state.in_(("toLaunch", "Running", "Resuming", "Terminated")))
        .group_by(Resource.network_address, JobType.type)
        .subquery()
    )

    # Main query to get only the rows with rn = 1 (max appearance count per network_address)
    results = (
        session.query(subquery.c.network_address, subquery.c.type, subquery.c.appearance_count)
        .filter(subquery.c.rn == 1)
        .order_by(asc(subquery.c.network_address))
        .all()
    )

    return results


def e_compact(session, itvs_slots, hy_res_rqts, hy, beginning_slotset, reverse=True, allocation=(1, 3, 2)):
    """
    Given a job resource request and a set of resources this function tries to find a matching allocation.

    .. note::
        This` can be override with the oar `extension <../admin/extensions.html#functions-assign-and-find>`_ mechanism.

    :param session: The DB session
    :param itvs_slots: A procset of the resources available for the allocation
    :type itvs_slots: :class:`procset.ProcSet`
    :param hy_res_rqts: The job's request
    :param hy: The definition of the resources hierarchy
    :return [ProcSet]: \
            The allocation if found, otherwise an empty :class:`procset.ProcSet`
    """
    # import time
    # start_time = time.time()

    logger.debug(__file__)
    config, db = init_oar(no_db=False)
    chars = get_nodes_characterization(session)

    rs = ResourceSet(session, config)

    nodes = {}
    list(map(lambda x: nodes.setdefault(x[1], []).append(x[0]), rs.roid_2_network_address.items()))

    nodes = {k: ProcSet(*v) for k, v in nodes.items()}

    agg = []
    for network_address in nodes.keys():
        _found = list(filter(lambda x: x[0] == network_address, chars))
        if _found:
            char = (
                allocation[0] if _found[0][1] == 'unfriendly'
                else allocation[1] if _found[0][1] == 'friendly'
                else allocation[2]
            )
        else:
            char = allocation[2]

        agg.append((nodes[network_address], len(nodes[network_address] & itvs_cts_slots),char))

    # end_time = time.time()
    # elapsed_time = end_time - start_time
    # logger.info(f"Function execution time: {elapsed_time:.4f} seconds")

    result = ProcSet()
    for hy_res_rqt in hy_res_rqts:
        (hy_level_nbs, constraints) = hy_res_rqt
        hy_levels = []
        hy_nbs = []
        for hy_l_n in hy_level_nbs:
            (l_name, n) = hy_l_n
            hy_levels.append(hy[l_name])
            hy_nbs.append(n)

        itvs_cts_slots = constraints & itvs_slots

        sorted_agg = sorted(
            agg,
            key=lambda x: (x[1], x[2]),
            reverse=reverse
        )

        logger.debug(sorted_agg)

        hy_nodes = list(map(lambda x: x[0],sorted_agg))

        hy_levels = []
        for node in hy_nodes:
            # collect cpu Procset for particular node
            n_cpus = list(filter(lambda p: p.issubset(node), hy["cpu"]))
            # sort cpu Procset list sorted by min/max free cores
            n_cpus = sorted(
                n_cpus, key=lambda i: len(i & itvs_cts_slots), reverse=reverse
            )
            # map cpu Procset to core procset
            hy_levels += list(
                map(ProcSet, itertools.chain.from_iterable(map(iter, n_cpus)))
            )

        # there is an Admission Rule that blocks other resources than core
        # so only 1 resource type will be given
        hy_levels = [hy_levels]

        res = find_resource_hierarchies_scattered(
            itvs_cts_slots, list(hy_levels), hy_nbs
        )
        if res:
            result = result | res
        else:
            return ProcSet()

    return result


def e_spread(session, itvs_slots, hy_res_rqts, hy, beginning_slotset, reverse=True, allocation=(1, 3, 2)):
    """
    Given a job resource request and a set of resources this function tries to find a matching allocation.

    .. note::
        This` can be override with the oar `extension <../admin/extensions.html#functions-assign-and-find>`_ mechanism.

    :param session: The DB session
    :param itvs_slots: A procset of the resources available for the allocation
    :type itvs_slots: :class:`procset.ProcSet`
    :param hy_res_rqts: The job's request
    :param hy: The definition of the resources hierarchy
    :return [ProcSet]: \
            The allocation if found, otherwise an empty :class:`procset.ProcSet`
    """
    result = ProcSet()
    for hy_res_rqt in hy_res_rqts:
        (hy_level_nbs, constraints) = hy_res_rqt
        hy_levels = []
        hy_nbs = []
        for hy_l_n in hy_level_nbs:
            (l_name, n) = hy_l_n
            hy_levels.append(hy[l_name])
            hy_nbs.append(n)

        itvs_cts_slots = constraints & itvs_slots

        itvs_cts_slots2 = itvs_cts_slots.copy()

        for soc in hy["cpu"]:
            avail_cores = soc & itvs_cts_slots
            itvs_cts_slots -= ProcSet(*avail_cores[int(len(soc) / 2) : len(soc)])

        logger.debug(__file__)
        config, db = init_oar(no_db=False)
        chars = get_nodes_characterization(session)

        rs = ResourceSet(session, config)

        nodes = {}
        list(map(lambda x: nodes.setdefault(x[1], []).append(x[0]), rs.roid_2_network_address.items()))

        nodes = {k: ProcSet(*v) for k, v in nodes.items()}

        agg = []
        for network_address in nodes.keys():
            _found = list(filter(lambda x: x[0] == network_address, chars))
            if _found:
                char = (
                    allocation[0] if _found[0][1] == 'unfriendly'
                    else allocation[1] if _found[0][1] == 'friendly'
                    else allocation[2]
                )
            else:
                char = allocation[2]

            agg.append((nodes[network_address], len(nodes[network_address] & itvs_cts_slots2),char))

        # Select unused resources first (top-down).
        try:
            # create a nodes Procset list sorted by min/max free cores
            sorted_agg = sorted(
                agg,
                key=lambda x: (x[1], x[2]),
                reverse=reverse
            )

            logger.debug(sorted_agg)

            hy_nodes = list(map(lambda x: x[0],sorted_agg))

            hy_levels = []
            for node in hy_nodes:
                # collect cpu Procset for particular node
                n_cpus = list(filter(lambda p: p.issubset(node), hy["cpu"]))
                # sort cpu Procset list sorted by min/max free cores
                n_cpus = sorted(
                    n_cpus, key=lambda i: len(i & itvs_cts_slots2), reverse=reverse
                )
                # map cpu Procset to core procset
                hy_levels += list(
                    map(ProcSet, itertools.chain.from_iterable(map(iter, n_cpus)))
                )

            # there is an Admission Rule that blocks other resources than core
            # so only 1 resource type will be given
            hy_levels = [hy_levels]

        except Exception as e:
            logger.info(e)

        res = find_resource_hierarchies_scattered(itvs_cts_slots, hy_levels, hy_nbs)

        if res:
            result = result | res
        else:
            return ProcSet()

    return result


def compact(session, itvs_slots, hy_res_rqts, hy, beginning_slotset, reverse=True):
    """
    Given a job resource request and a set of resources this function tries to find a matching allocation.

    .. note::
        This` can be override with the oar `extension <../admin/extensions.html#functions-assign-and-find>`_ mechanism.

    :param session: The DB session
    :param itvs_slots: A procset of the resources available for the allocation
    :type itvs_slots: :class:`procset.ProcSet`
    :param hy_res_rqts: The job's request
    :param hy: The definition of the resources hierarchy
    :return [ProcSet]: \
            The allocation if found, otherwise an empty :class:`procset.ProcSet`
    """
    # logger.info(session)

    # queryCollection = BaseQueryCollection(session)
    # jobs=get_jobs_in_state(session,'Running')
    # logger.info(jobs)
    # res = queryCollection.get_assigned_jobs_resources(jobs)
    # logger.info(res)

    result = ProcSet()
    for hy_res_rqt in hy_res_rqts:
        (hy_level_nbs, constraints) = hy_res_rqt
        hy_levels = []
        hy_nbs = []
        for hy_l_n in hy_level_nbs:
            (l_name, n) = hy_l_n
            hy_levels.append(hy[l_name])
            hy_nbs.append(n)

        itvs_cts_slots = constraints & itvs_slots

        # create a nodes Procset list sorted by min/max free cores
        hy_nodes = sorted(
            hy["network_address"],
            key=lambda i: len(i & itvs_cts_slots),
            reverse=reverse,
        )

        hy_levels = []
        for node in hy_nodes:
            # collect cpu Procset for particular node
            n_cpus = list(filter(lambda p: p.issubset(node), hy["cpu"]))
            # sort cpu Procset list sorted by min/max free cores
            n_cpus = sorted(
                n_cpus, key=lambda i: len(i & itvs_cts_slots), reverse=reverse
            )
            # map cpu Procset to core procset
            hy_levels += list(
                map(ProcSet, itertools.chain.from_iterable(map(iter, n_cpus)))
            )

        # there is an Admission Rule that blocks other resources than core
        # so only 1 resource type will be given
        hy_levels = [hy_levels]

        res = find_resource_hierarchies_scattered(
            itvs_cts_slots, list(hy_levels), hy_nbs
        )
        if res:
            result = result | res
        else:
            return ProcSet()

    return result


def spread(session, itvs_slots, hy_res_rqts, hy, beginning_slotset, reverse=True):
    """
    Given a job resource request and a set of resources this function tries to find a matching allocation.

    .. note::
        This` can be override with the oar `extension <../admin/extensions.html#functions-assign-and-find>`_ mechanism.

    :param session: The DB session
    :param itvs_slots: A procset of the resources available for the allocation
    :type itvs_slots: :class:`procset.ProcSet`
    :param hy_res_rqts: The job's request
    :param hy: The definition of the resources hierarchy
    :return [ProcSet]: \
            The allocation if found, otherwise an empty :class:`procset.ProcSet`
    """
    result = ProcSet()
    for hy_res_rqt in hy_res_rqts:
        (hy_level_nbs, constraints) = hy_res_rqt
        hy_levels = []
        hy_nbs = []
        for hy_l_n in hy_level_nbs:
            (l_name, n) = hy_l_n
            hy_levels.append(hy[l_name])
            hy_nbs.append(n)

        itvs_cts_slots = constraints & itvs_slots

        itvs_cts_slots2 = itvs_cts_slots.copy()

        for soc in hy["cpu"]:
            avail_cores = soc & itvs_cts_slots
            itvs_cts_slots -= ProcSet(*avail_cores[int(len(soc) / 2) : len(soc)])

        # Select unused resources first (top-down).
        try:
            # create a nodes Procset list sorted by min/max free cores
            hy_nodes = sorted(
                hy["network_address"],
                key=lambda i: len(i & itvs_cts_slots2),
                reverse=reverse,
            )

            hy_levels = []
            for node in hy_nodes:
                # collect cpu Procset for particular node
                n_cpus = list(filter(lambda p: p.issubset(node), hy["cpu"]))
                # sort cpu Procset list sorted by min/max free cores
                n_cpus = sorted(
                    n_cpus, key=lambda i: len(i & itvs_cts_slots2), reverse=reverse
                )
                # map cpu Procset to core procset
                hy_levels += list(
                    map(ProcSet, itertools.chain.from_iterable(map(iter, n_cpus)))
                )

            # there is an Admission Rule that blocks other resources than core
            # so only 1 resource type will be given
            hy_levels = [hy_levels]

        except Exception as e:
            logger.info(e)

        res = find_resource_hierarchies_scattered(itvs_cts_slots, hy_levels, hy_nbs)

        if res:
            result = result | res
        else:
            return ProcSet()

    return result

def co_loc(session, itvs_slots, hy_res_rqts, hy, beginning_slotset):
    """
    Given a job resource request and a set of resources this function tries to find a matching allocation.

    .. note::
        This` can be override with the oar `extension <../admin/extensions.html#functions-assign-and-find>`_ mechanism.

    :param session: The DB session
    :param itvs_slots: A procset of the resources available for the allocation
    :type itvs_slots: :class:`procset.ProcSet`
    :param hy_res_rqts: The job's request
    :param hy: The definition of the resources hierarchy
    :return [ProcSet]: \
            The allocation if found, otherwise an empty :class:`procset.ProcSet`
    """
    return spread(session, itvs_slots, hy_res_rqts, hy, beginning_slotset, reverse=False)


def no_pref(session, itvs_slots, hy_res_rqts, hy, beginning_slotset):
    """
    Given a job resource request and a set of resources this function tries to find a matching allocation.

    .. note::
        This` can be override with the oar `extension <../admin/extensions.html#functions-assign-and-find>`_ mechanism.

    :param session: The DB session
    :param itvs_slots: A procset of the resources available for the allocation
    :type itvs_slots: :class:`procset.ProcSet`
    :param hy_res_rqts: The job's request
    :param hy: The definition of the resources hierarchy
    :return [ProcSet]: \
            The allocation if found, otherwise an empty :class:`procset.ProcSet`
    """
    return compact(session, itvs_slots, hy_res_rqts, hy, beginning_slotset, reverse=False)


def f_spread(session, itvs_slots, hy_res_rqts, hy, beginning_slotset, reverse=True):
    """
    Given a job resource request and a set of resources this function tries to find a matching allocation.

    .. note::
        This` can be override with the oar `extension <../admin/extensions.html#functions-assign-and-find>`_ mechanism.

    :param session: The DB session
    :param itvs_slots: A procset of the resources available for the allocation
    :type itvs_slots: :class:`procset.ProcSet`
    :param hy_res_rqts: The job's request
    :param hy: The definition of the resources hierarchy
    :return [ProcSet]: \
            The allocation if found, otherwise an empty :class:`procset.ProcSet`
    """
    avail_procset = spread(
        session, itvs_slots, hy_res_rqts, hy, beginning_slotset, reverse=reverse
    )

    # if no allocation space is found (by compact policy)
    # fallback to compact/no_pref
    if len(avail_procset) == 0:
        return compact(session, itvs_slots, hy_res_rqts, hy, beginning_slotset, reverse=reverse)
    else:
        return avail_procset


def f_co_loc(session, itvs_slots, hy_res_rqts, hy, beginning_slotset):
    """
    Given a job resource request and a set of resources this function tries to find a matching allocation.

    .. note::
        This` can be override with the oar `extension <../admin/extensions.html#functions-assign-and-find>`_ mechanism.

    :param session: The DB session
    :param itvs_slots: A procset of the resources available for the allocation
    :type itvs_slots: :class:`procset.ProcSet`
    :param hy_res_rqts: The job's request
    :param hy: The definition of the resources hierarchy
    :return [ProcSet]: \
            The allocation if found, otherwise an empty :class:`procset.ProcSet`
    """
    return f_spread(session, itvs_slots, hy_res_rqts, hy, beginning_slotset, reverse=False)
